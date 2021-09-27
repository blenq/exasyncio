""" exasyncio connection

Manage a connection to an Exacol database

"""
from asyncio import Lock, get_running_loop
import base64
import getpass
import json
import os
import platform
import zlib

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

import aiohttp
import rsa

from .common import ExaConnStatus, AsyncContextMixin
from .resultset import Result

_upper_zones = {z.upper(): z for z in zoneinfo.available_timezones()}


class ExaError(Exception):
    """ Base class for Exasol Exceptions """


class ExaProtocolError(ExaError):
    """ Indicates an error in the communication protocol. Should not occur """


class ExaServerError(ExaError):
    """ Encapsulates an error reported by the Exasol server """

    @property
    def code(self):
        """ error code """
        return self.args[0]

    @property
    def message(self):
        """ error message """
        return self.args[1]


def _from_arg_or_env(name, val, default=None):
    if val is not None:
        return val
    return os.environ.get(f"EXA{name}", default)


class Connection(AsyncContextMixin):
    """ Represents an Exasol database connection using the websockets protocol.

    """
    def __init__(
            self, host=None, port=None, user=None, password=None,
            schema='', autocommit=True, query_timeout=0,
            snapshot_transactions=None, use_compression=True):

        host = _from_arg_or_env("HOST", host)
        if host is None:
            raise ValueError("Missing host")
        port = _from_arg_or_env("PORT", port, 8563)
        user = _from_arg_or_env("USER", user)
        password = _from_arg_or_env("PASSWORD", password)
        uri = f"ws://{host}:{port}"

        self.uri = uri
        self.user = user
        self.__password = password.encode()
        self.schema = schema
        self._autocommit = autocommit
        self.query_timeout = query_timeout
        self.snapshot_transactions = snapshot_transactions
        self.status = ExaConnStatus.CLOSED
        self.date_format = None
        self.datetime_format = None
        self._use_compression = use_compression
        self._tz = None
        self._ws = None
        self._req_lock = Lock()
        self._session = None
        self._ws = None
        self._loop = None
        self._recv_msg = self._recv_msg_uncompressed
        self._send_msg = self._send_msg_uncompressed

    @property
    def autocommit(self):
        """ Indicates if autocommit is enabled """
        return self._autocommit

    async def _connect(self):
        if self.status is not ExaConnStatus.CLOSED:
            raise ValueError("Connection is already connected")

        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.uri)
        # self.ws = await ws_connect(self.uri)
        self.status = ExaConnStatus.WS_CONNECTED

    async def _recv_msg_uncompressed(self):
        msg = await self._ws.receive()
        return msg.data

    async def _recv_msg_compressed(self):
        data = await self._recv_msg_uncompressed()
        return zlib.decompress(data).decode()

    async def _recv(self):
        data = await self._recv_msg()
        try:
            data = json.loads(data)
        except BaseException as ex:
            raise ExaProtocolError("Invalid json") from ex

        attrs = data.get("attributes")
        if attrs is not None:
            self.date_format = attrs.get("dateFormat", self.date_format)
            self.datetime_format = attrs.get(
                "datetimeFormat", self.datetime_format)
            tz_name = attrs.get("timezone")
            if tz_name is not None:
                tz_name = _upper_zones.get(tz_name)
                if tz_name is None:
                    self._tz = None
                else:
                    self._tz = zoneinfo.ZoneInfo(tz_name)
            self._autocommit = attrs.get("autocommit", self._autocommit)
        try:
            status = data["status"]
        except BaseException as ex:
            raise ExaProtocolError("Error retrieving status") from ex

        if status == "ok":
            return data
        if status == "error":
            try:
                ex_data = data["exception"]
                sql_code = ex_data["sqlCode"]
                text = ex_data["text"]
            except KeyError as ex:
                raise ExaProtocolError(
                    "Invalid or missing exception data") from ex
            raise ExaServerError(sql_code, text)
        raise ExaProtocolError("Invalid status")

    async def _send_msg_compressed(self, data):
        await self._ws.send_bytes(zlib.compress(data.encode(), 1))

    async def _send_msg_uncompressed(self, data):
        await self._ws.send_str(data)

    async def _request(self, data):
        async with self._req_lock:
            await self._send_msg(json.dumps(data))
            return await self._recv()

    def _encrypt_password(self, public_key_pem):
        pub_key = rsa.PublicKey.load_pkcs1(public_key_pem.encode())
        return base64.b64encode(rsa.encrypt(self.__password, pub_key)).decode()

    async def _login(self):
        if self.status is not ExaConnStatus.WS_CONNECTED:
            if self.status is ExaConnStatus.CLOSED:
                err_msg = "Connection is closed"
            else:
                err_msg = "Connection is logged in"
            raise ValueError(err_msg)

        resp = await self._request({'command': 'login', 'protocolVersion': 3})
        password = self._encrypt_password(resp['responseData']['publicKeyPem'])
        resp = await self._request({
            'username': self.user,
            'password': password,
            'driverName': 'exasyncio 0.1',
            'clientName': 'exasyncio',
            'clientVersion': '0.1',
            'clientOs': platform.platform(),
            'clientOsUsername': getpass.getuser(),
            'clientRuntime': f'Python {platform.python_version()}',
            'useCompression': self._use_compression,
            'attributes': self._get_login_attributes()
        })
        if self._use_compression:
            self._recv_msg = self._recv_msg_compressed
            self._send_msg = self._send_msg_compressed
        self.date_format = 'YYYY-MM-DD'
        self.status = ExaConnStatus.CONNECTED

    def _get_login_attributes(self):
        attrs = {
            'currentSchema': self.schema,
            'autocommit': self.autocommit,
            'queryTimeout': self.query_timeout,
            'dateFormat': 'YYYY-MM-DD',
        }

        if self.snapshot_transactions is not None:
            attrs['snapshotTransactionsEnabled'] = self.snapshot_transactions

        return attrs

    async def _get_attributes(self):
        await self._request({"command": "getAttributes"})

    def __await__(self):

        async def _await():
            self._loop = get_running_loop()
            try:
                await self._connect()
                await self._login()
                await self._get_attributes()
            except BaseException:
                await self.close()
                raise
            return self

        return _await().__await__()

    async def execute(self, query, raw=False):
        """ Executes a query and returns the result """
        resp = await self._request({
            'command': 'execute',
            'sqlText': query,
        })
        return Result(self, resp["responseData"], raw)

    async def _fetch(self, handle, offset):
        return await self._request({
            'command': 'fetch',
            'resultSetHandle': handle,
            'startPosition': offset,
            'numBytes': 5242880,  # 5 MB
        })

    async def _close_result(self, handle):
        await self._close_results([handle])

    async def _close_results(self, handles):
        if self.status is not ExaConnStatus.CONNECTED:
            return
        await self._request({
            'command': 'closeResultSet', 'resultSetHandles': handles,
        })

    async def close(self):
        """ Closes the connection. Can be called multiple times """

        if self.status is ExaConnStatus.CONNECTED:
            self.status = ExaConnStatus.DISCONNECTING
            try:
                await self._request({'command': 'disconnect'})
            except BaseException:  # noqa
                pass
        self.status = ExaConnStatus.CLOSING
        websocket = self._ws
        if websocket is not None:
            self._ws = None
            await websocket.close()
        self.status = ExaConnStatus.CLOSED
        session = self._session
        if session is not None:
            self._session = None
            await session.close()
