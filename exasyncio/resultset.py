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

from .common import ExaConnStatus, AsyncContextMixin

datetime_pattern = re.compile(
    r"YYYY-MM-DD(( |T)HH(24)?(:MI(:SS(\.FF(3|6))?)?)?)?")


class ResultType(Enum):
    """ Indicates the type of result """
    RESULTSET = 'resultSet'
    ROWCOUNT = 'rowCount'


def _get_hash(val):
    """ Converter for HASHTYPE values """
    if '-' in val:
        return UUID(val)
    return bytes.fromhex(val)


class Result(AsyncContextMixin):
    """ Represents the result of a query. Instantiated by Connection.execute.

    """
    def __init__(self, cn, data, raw):
        self._cn = cn
        self._datetime_format = cn.datetime_format
        self._date_format = cn.date_format
        self._aiterator = None
        result_data = data["results"][0]
        self.result_type = ResultType(result_data["resultType"])
        if self.result_type is ResultType.RESULTSET:
            result_data = result_data['resultSet']
            self._data = result_data.get("data")
            self._result_handle = result_data.get("resultSetHandle")
            self.rowcount = result_data["numRows"]
            if raw:
                # no transformation requested, besides swapping columns to
                # rows
                self._transform = zip
            else:
                self._tz = cn._tz
                converters = list(
                    self._get_converters(result_data["columns"]))
                if all(c is None for c in converters):
                    # no converters necessary, shortcut to zip
                    self._transform = zip
                else:
                    self._converters = converters
                    self._transform = self._transform_rows
        else:
            self._data = None
            self._result_handle = None

    def _get_datetime_tz(self, val):
        """ Converter for datetimes with time zone """

        return datetime.fromisoformat(val).replace(tzinfo=self._tz)

    @cached_property
    def _can_parse_datetime(self):
        return bool(datetime_pattern.fullmatch(self._datetime_format))

    def _get_converter(self, col_data):
        """ Returns a converter based on the column metadata """

        data_type = col_data["type"]

        if data_type == 'DECIMAL':
            if col_data["scale"] == 0:
                if col_data["precision"] < 19:
                    return None
                return int
            return Decimal

        if data_type == 'DATE':
            if self._date_format == 'YYYY-MM-DD':
                return date.fromisoformat

        elif data_type == 'TIMESTAMP WITH LOCAL TIME ZONE':
            if self._can_parse_datetime:
                if self._tz:
                    return self._get_datetime_tz
                return datetime.fromisoformat

        elif data_type == 'TIMESTAMP':
            if self._can_parse_datetime:
                return datetime.fromisoformat

        elif data_type == 'HASHTYPE':
            return _get_hash

        return None

    def _get_converters(self, columns):
        for col in columns:
            yield self._get_converter(col["dataType"])

    def _transform_rows(self, *data):
        return (
            tuple(
                val if conv is None else conv(val)
                for conv, val in zip(self._converters, row))
            for row in zip(*data)
        )

    async def _aiterate(self):
        if self.result_type is not ResultType.RESULTSET:
            raise ValueError("Result has no data")
        if self._result_handle is not None:
            # result(s) not present in data, fetch result data
            num_rows = 0
            while num_rows < self.rowcount:
                resp_data = (await self._cn._fetch(
                    self._result_handle, num_rows))["responseData"]
                for row in self._transform(*resp_data["data"]):
                    yield row
                num_rows += resp_data["numRows"]
        elif self._data is not None:
            # single result, already present in data
            for row in self._transform(*self._data):
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
            await self._cn._close_result(result_handle)
        if self._data is not None:
            self._data = None
        if self._aiterator is not None:
            await self._aiterator.aclose()

    def __del__(self):
        result_handle = self._result_handle
        if (result_handle is not None and
                self._cn.status is ExaConnStatus.CONNECTED and
                not self._cn._loop.is_closed()):
            # Result is not fully consumed and not closed server-side yet. It
            # can not be closed now, because this is a synchronous method and
            # closing a statement is an async operation. The GC might also be
            # running from a different thread.
            # Therefore use run_coroutine_threadsafe to close the result handle
            self._result_handle = None
            run_coroutine_threadsafe(
                self._cn._close_result(result_handle), self._cn._loop)
