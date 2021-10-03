""" exasyncio result

Manage the results of executed Exasol queries

"""
from asyncio import run_coroutine_threadsafe
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import cached_property
import re

from uuid import UUID

from exasyncio.common import ExaConnStatus, AsyncContextMixin

datetime_pattern = re.compile(
    r"YYYY-MM-DD(( |T)HH(24)?(:MI(:SS(\.FF(3|6))?)?)?)?")


class ResultType(Enum):
    """ Indicates the type of result """
    RESULTSET = 'resultSet'
    ROWCOUNT = 'rowCount'


def _get_decimal_converter(col_data):
    if col_data["scale"] == 0:
        if col_data["precision"] < 19:
            return None
        return int
    return Decimal


def _get_bytes_or_uuid(val):
    """ Converter for HASHTYPE values """

    # This converter is used when the data type size equals 32 (=16 bytes hex).
    # It will return a UUID, when the actual size equals 36, which is 32 hex
    # characters + 4 dashes. That is the case when the HASHTYPE_FORMAT is set
    # to 'UUID'. Therefore, return a UUID in that case.
    if len(val) == 36:
        return UUID(val)
    return bytes.fromhex(val)


def _get_hashtype_converter(col_data):
    if col_data["size"] == 32:
        return _get_bytes_or_uuid
    return bytes.fromhex


class ResultConverter:
    """ Helper class to convert column data """

    def __init__(self, result):
        conn = result.connection
        self.tzinfo = conn._tz
        self.datetime_format = conn.datetime_format
        self.date_format = conn.date_format
        self.columns = result.columns

    # pylint: disable-next=unused-argument
    def get_date_converter(self, col_data):  # noqa
        """ Returns converter funtion for date values """
        if self.date_format == 'YYYY-MM-DD':
            return date.fromisoformat
        return None

    @cached_property
    def can_parse_datetime(self):
        """ Indicates if the datetime format is ok for datetime.fromisoformat
        """
        return bool(datetime_pattern.fullmatch(self.datetime_format))

    # pylint: disable-next=unused-argument
    def get_timestamp_converter(self, col_data):  # noqa
        """ Returns converter function for timestamp values """
        if self.can_parse_datetime:
            return datetime.fromisoformat
        return None

    def get_datetime_tz(self, val):
        """ Converter for datetimes with time zone """

        return datetime.fromisoformat(val).replace(tzinfo=self.tzinfo)

    # pylint: disable-next=unused-argument
    def get_timestamptz_converter(self, col_data):  # noqa
        """ Returns converter function for timestamptz values """
        if self.can_parse_datetime:
            if self.tzinfo:
                return self.get_datetime_tz
            return datetime.fromisoformat
        return None

    def get_transform(self):
        """ Returns the function to use for transforming the column data """

        # Getting a converter for a column is a two step process. First a
        # converter_getter is retrieved for the data type name.
        # The converter_getter is responsible for returning the actual
        # converter function, if any.
        type_conv_getters = {
            "DECIMAL": _get_decimal_converter,
            "DATE": self.get_date_converter,
            "TIMESTAMP": self.get_timestamp_converter,
            "TIMESTAMP WITH LOCAL TIME ZONE": self.get_timestamptz_converter,
            "HASHTYPE": _get_hashtype_converter,
        }
        col_types = [col["dataType"] for col in self.columns]
        converter_getters = (
            type_conv_getters.get(col_type["type"]) for col_type in col_types)
        converters = [
            get_conv and get_conv(col_type)
            for col_type, get_conv in zip(col_types, converter_getters)]

        if all(c is None for c in converters):
            # no converters present, shortcut to zip
            return zip

        def transform_rows(*data):
            # uses the earlier retrieved converters to transform each row
            return (
                tuple(
                    val if conv is None else conv(val)
                    for conv, val in zip(converters, row))
                for row in zip(*data)
            )

        return transform_rows


class Result(AsyncContextMixin):
    """ Represents the result of a query. Instantiated by Connection.execute.

    """
    def __init__(self, cn, data, raw):
        self._cn = cn
        self._aiterator = None
        result_data = data["results"][0]
        self.result_type = ResultType(result_data["resultType"])
        if self.result_type is ResultType.RESULTSET:
            self._result_set = result_data['resultSet']
            result_data = result_data['resultSet']
            self._data = result_data.get("data")
            self._result_handle = result_data.get("resultSetHandle")
            self.rowcount = result_data["numRows"]
            if raw:
                self._result_converter = None
            else:
                self._result_converter = ResultConverter(self)
        else:
            self._data = None
            self._result_handle = None

    @property
    def connection(self):
        return self._cn

    @cached_property
    def columns(self):
        if self.result_type is ResultType.RESULTSET:
            return self._result_set["columns"]
        return None

    async def _aiterate(self):
        if self.result_type is not ResultType.RESULTSET:
            raise ValueError("Result has no data")

        if self._result_converter is None:
            transform = zip
        else:
            transform = self._result_converter.get_transform()

        if self._result_handle is not None:
            # result(s) not present in data, fetch result data
            num_rows = 0
            while num_rows < self.rowcount:
                # pylint: disable-next=protected-access
                resp_data = (await self.connection._fetch(
                    self._result_handle, num_rows))["responseData"]
                for row in transform(*resp_data["data"]):
                    yield row
                num_rows += resp_data["numRows"]
        elif self._data is not None:
            # single result, already present in data
            for row in transform(*self._data):
                yield row
        # fully iterated over result so close immediately
        await self.close()

    def __aiter__(self):
        # Iterating is a single forward only operation. Iterating a second time
        # over the result will not yield any rows.

        if self._aiterator is None:
            self._aiterator = self._aiterate()
        return self._aiterator

    async def fetchone(self):
        """ Returns the next row of the result as a tuple if available, else
        None

        """
        async for row in self:
            return row

    async def fetchall(self):
        """ Returns a list of the remaining rows as tuples """
        return [row async for row in self]

    async def close(self):
        """ Closes the result. Can be called multiple times """
        result_handle = self._result_handle
        if result_handle is not None:
            self._result_handle = None
            # pylint: disable-next=protected-access
            await self.connection._close_result(result_handle)
        if self._data is not None:
            self._data = None
        if self._aiterator is not None:
            await self._aiterator.aclose()

    def __del__(self):
        result_handle = self._result_handle
        if (result_handle is not None and
                self.connection.status is ExaConnStatus.CONNECTED and
                not self.connection._loop.is_closed()):

            self._result_handle = None
            # Result is not fully consumed and not closed server-side yet. It
            # can not be closed now, because this is a synchronous method and
            # closing a statement is an async operation. The GC might also be
            # running from a different thread.
            # Therefore use run_coroutine_threadsafe to close the result handle
            run_coroutine_threadsafe(
                self.connection._close_result(result_handle),
                self.connection._loop)
