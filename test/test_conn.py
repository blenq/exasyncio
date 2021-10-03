import json
import os
from unittest import IsolatedAsyncioTestCase

from exasyncio import (
    Connection, ExaConnStatus, ExaServerError,
    ExaProtocolError)


def _from_env(name):
    return os.environ[f"EXATEST{name}"]


exa_host = _from_env("HOST")
exa_user = _from_env("USER")
exa_password = _from_env("PASSWORD")


class ConnTestCase(IsolatedAsyncioTestCase):

    async def test_connect(self):
        cn = await Connection(
            exa_host, user=exa_user, password=exa_password)
        self.assertIs(cn.status, ExaConnStatus.CONNECTED)
        await cn.close()

    async def test_connect_using_environ(self):
        os.environ["EXAHOST"] = exa_host
        os.environ["EXAUSER"] = exa_user
        os.environ["EXAPASSWORD"] = exa_password
        cn = await Connection()
        self.assertIs(cn.status, ExaConnStatus.CONNECTED)
        await cn.close()
        del os.environ["EXAHOST"]
        del os.environ["EXAUSER"]
        del os.environ["EXAPASSWORD"]

    async def test_wrong_credentials(self):
        with self.assertRaises(ExaServerError) as ex:
            await Connection(
                exa_host, user=exa_user, password=exa_password + '1')
        self.assertEqual(ex.exception.code, "08004")
        self.assertIsNot(ex.exception.message, None)

    async def test_without_host(self):
        with self.assertRaises(ValueError):
            await Connection(user=exa_user, password=exa_password)

    async def test_connect_twice(self):
        cn = await Connection(
            exa_host, user=exa_user, password=exa_password)
        with self.assertRaises(ValueError):
            await cn

    async def test_without_compression(self):
        cn = await Connection(
            exa_host, user=exa_user, password=exa_password,
            use_compression=False)
        self.assertIs(cn.status, ExaConnStatus.CONNECTED)
        await cn.close()

    async def test_close_twice(self):
        async with await Connection(
                exa_host, user=exa_user, password=exa_password,
                use_compression=False) as cn:
            self.assertIs(cn.status, ExaConnStatus.CONNECTED)
        await cn.close()
        self.assertIs(cn.status, ExaConnStatus.CLOSED)

    async def test_invalid_json(self):
        cn = Connection(
            exa_host, user=exa_user, password=exa_password)

        old_recv = cn._recv_msg

        async def recv():
            await old_recv()
            return "nonsense"

        cn._recv_msg = recv
        with self.assertRaises(ExaProtocolError):
            await cn

    async def test_unknown_timezone(self):
        async with Connection(
                exa_host, user=exa_user, password=exa_password,
                use_compression=False) as cn:

            old_recv = cn._recv_msg

            async def recv():
                data = await old_recv()
                json_data = json.loads(data)
                attrs = json_data.get('attributes')
                if attrs is not None:
                    attrs["timezone"] = "Europe/Chicago"
                    data = json.dumps(json_data)
                return data

            cn._recv_msg = recv
            await cn
            self.assertIsNone(cn._tz, None)

    async def test_missing_response_status(self):
        async with Connection(
                exa_host, user=exa_user, password=exa_password) as cn:

            old_recv = cn._recv_msg

            async def recv():
                data = await old_recv()
                json_data = json.loads(data)
                del json_data["status"]
                return json.dumps(json_data)

            cn._recv_msg = recv
            with self.assertRaises(ExaProtocolError):
                await cn

    async def test_wrong_response_status(self):
        async with Connection(
                exa_host, user=exa_user, password=exa_password,
                use_compression=False) as cn:

            old_recv = cn._recv_msg

            async def recv():
                data = await old_recv()
                json_data = json.loads(data)
                json_data["status"] = "nonsense"
                return json.dumps(json_data)

            cn._recv_msg = recv
            with self.assertRaises(ExaProtocolError):
                await cn

    async def test_wrong_error_data(self):
        async with Connection(
                exa_host, user=exa_user, password=exa_password + '1',
                use_compression=False) as cn:

            old_recv = cn._recv_msg

            async def recv():
                data = await old_recv()
                json_data = json.loads(data)
                if json_data["status"] == "error":
                    del json_data["exception"]["sqlCode"]
                return json.dumps(json_data)

            cn._recv_msg = recv
            with self.assertRaises(ExaProtocolError):
                await cn

    async def test_closed_conn_close_result(self):
        cn = await Connection(
            exa_host, user=exa_user, password=exa_password,
            snapshot_transactions=True)
        res = await cn.execute(
            "SELECT * FROM EXA_TIME_ZONES, EXA_TIME_ZONES")
        await cn.close()
        await res.close()
