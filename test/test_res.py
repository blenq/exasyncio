from datetime import date, datetime
from decimal import Decimal
from unittest import IsolatedAsyncioTestCase
from uuid import UUID

try:
    import zoneinfo
except ImportError:
    from backports import zoneinfo

from exasyncio import Connection, ResultType

from .test_conn import exa_host, exa_user, exa_password


class ResTestCase(IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.cn = await Connection(
            exa_host, user=exa_user, password=exa_password)

    async def test_int(self):
        res = await self.cn.execute("SELECT 3")
        self.assertEqual((await res.fetchone())[0], 3)

    async def test_large_int(self):
        res = await self.cn.execute(
            "SELECT 9999999999999999999999999999999999")
        self.assertEqual(
            (await res.fetchone())[0], 9999999999999999999999999999999999)

    async def test_decimal(self):
        res = await self.cn.execute("SELECT 3.5")
        val = (await res.fetchone())[0]
        self.assertEqual(val, Decimal('3.5'))
        self.assertIsInstance(val, Decimal)

    async def test_date(self):
        await self.cn.execute(
            "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'")
        res = await self.cn.execute("SELECT CAST('2020-05-23' AS DATE)")
        val = (await res.fetchone())[0]
        self.assertEqual(val, date(2020, 5, 23))

        await self.cn.execute(
            "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-DDD'")
        res = await self.cn.execute("SELECT CAST('2020-158' AS DATE)")
        val = (await res.fetchone())[0]
        self.assertEqual(val, '2020-158')

    async def test_timestamp(self):
        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH:MI:SS.FF6'")
        res = await self.cn.execute(
            "SELECT CAST('2020-05-23 14:12:12.345000' AS TIMESTAMP)")
        dt = datetime(2020, 5, 23, 14, 12, 12, 345000)
        val = (await res.fetchone())[0]
        self.assertEqual(val, dt)

        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH:MI:SS.FF3'")
        res = await self.cn.execute(
            "SELECT CAST('2020-05-23 14:12:12.345' AS TIMESTAMP)")
        val = (await res.fetchone())[0]
        self.assertEqual(val, dt)

        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS'")
        res = await self.cn.execute(
            "SELECT CAST('2020-05-23 14:12:12' AS TIMESTAMP)")
        val = (await res.fetchone())[0]
        self.assertEqual(val, datetime(2020, 5, 23, 14, 12, 12))

        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI'")
        res = await self.cn.execute(
            "SELECT CAST('2020-05-23 14:12' AS TIMESTAMP)")
        val = (await res.fetchone())[0]
        self.assertEqual(val, datetime(2020, 5, 23, 14, 12))

        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DDTHH24'")
        res = await self.cn.execute(
            "SELECT CAST('2020-05-23T14' AS TIMESTAMP)")
        val = (await res.fetchone())[0]
        self.assertEqual(val, datetime(2020, 5, 23, 14))

        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-DDDTHH24'")
        res = await self.cn.execute(
            "SELECT CAST('2020-158T14' AS TIMESTAMP)")
        val = (await res.fetchone())[0]
        self.assertEqual(val, '2020-158T14')

    async def test_timestamp_tz(self):
        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH:MI:SS.FF6'")
        await self.cn.execute("ALTER SESSION SET TIME_ZONE='Europe/Amsterdam'")
        res = await self.cn.execute("""
            SELECT CAST('2020-05-23 14:12:12.345000' AS
                         TIMESTAMP WITH LOCAL TIME ZONE)""")
        dt = datetime(
            2020, 5, 23, 14, 12, 12, 345000,
            zoneinfo.ZoneInfo('Europe/Amsterdam'))
        val = (await res.fetchone())[0]
        self.assertEqual(val, dt)

        try:
            tz, self.cn._tz = self.cn._tz, None
            res = await self.cn.execute("""
                SELECT CAST('2020-05-23 14:12:12.345000' AS
                             TIMESTAMP WITH LOCAL TIME ZONE)""")
            val = (await res.fetchone())[0]
        finally:
            self.cn_tz = tz
        self.assertEqual(val, datetime(2020, 5, 23, 14, 12, 12, 345000))

        await self.cn.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-DDD HH:MI:SS.FF6'")
        res = await self.cn.execute("""
            SELECT CAST('2020-158 14:12:12.345000' AS
                         TIMESTAMP WITH LOCAL TIME ZONE)""")
        val = (await res.fetchone())[0]
        self.assertEqual(val, '2020-158 14:12:12.345000')

    async def test_hash(self):
        await self.cn.execute("ALTER SESSION SET HASHTYPE_FORMAT='uuid'")
        res = await self.cn.execute("""
            SELECT CAST('550e8400-e29b-11d4-a716-446655440099'
                        AS HASHTYPE(16 BYTE))""")
        val = (await res.fetchall())[0][0]
        self.assertEqual(UUID('550e8400-e29b-11d4-a716-446655440099'), val)

        res = await self.cn.execute("""
            SELECT CAST('550e8400-e29b-11d4-a716-4466554400'
                        AS HASHTYPE(15 BYTE))""")
        val = (await res.fetchall())[0][0]
        self.assertEqual(bytes.fromhex('550e8400e29b11d4a7164466554400'), val)

        await self.cn.execute("ALTER SESSION SET HASHTYPE_FORMAT='HEX'")
        res = await self.cn.execute("""
            SELECT CAST('550e8400-e29b-11d4-a716-446655440099'
                        AS HASHTYPE(16 BYTE))""")
        val = (await res.fetchall())[0][0]
        self.assertEqual(
            bytes.fromhex('550e8400e29b11d4a716446655440099'), val)

    async def test_raw(self):
        await self.cn.execute(
            "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'")
        res = await self.cn.execute(
            "SELECT CAST('2020-05-23' AS DATE)", raw=True)
        val = (await res.fetchone())[0]
        self.assertEqual(val, '2020-05-23')

    async def test_no_iterator(self):
        res = await self.cn.execute("ALTER SESSION SET HASHTYPE_FORMAT='uuid'")
        self.assertEqual(res.result_type, ResultType.ROWCOUNT)
        with self.assertRaises(ValueError):
            await res.fetchall()

    async def test_iterate_twice(self):
        res = await self.cn.execute("SELECT 1")
        rows = await res.fetchall()
        self.assertEqual(rows, [(1,)])
        rows = await res.fetchall()
        self.assertEqual(rows, [])
        self.assertIsNone(await res.fetchone())

    async def test_ctx(self):
        async with await self.cn.execute("SELECT 1") as res:
            pass
        self.assertIsNone(res._data)

    async def test_fetch(self):
        res = await self.cn.execute("SELECT COUNT(*) FROM EXA_TIME_ZONES")
        cnt = (await res.fetchone())[0]
        res = await self.cn.execute(
            "SELECT * FROM EXA_TIME_ZONES, EXA_TIME_ZONES")
        rows = await res.fetchall()
        self.assertEqual(len(rows), cnt * cnt)
        await res.close()

    async def test_fetch_del(self):
        res = await self.cn.execute(
            "SELECT * FROM EXA_TIME_ZONES, EXA_TIME_ZONES")
        del res

    async def test_invalid_iterate(self):
        res = await self.cn.execute("SELECT 1")
        await res.fetchall()
        self.assertEqual([row async for row in res._aiterate()], [])

    async def test_close(self):
        res = await self.cn.execute("SELECT 1")
        await res.close()
        self.assertIsNone(res._data)
        await res.close()

    async def asyncTearDown(self):
        await self.cn.close()
