# exasyncio

A python client library for the Exasol database, using the asyncio framework.

This is a minimal implementation, of the Exasol websocket protocol. Queries
can be executed and the results examined. 

## install

If you want to give it a test run, create and activate a python virtual
environment. (Note: tested with Python 3.8 and 3.9)

Upgrade the virtualenv and install the wheel format package

```shell
pip install --upgrade pip setuptools wheel
```

Install exasyncio

```shell
pip install git+https://github.com/blenq/exasyncio.git
```

## use it

```python
import asyncio

from exasyncio import Connection


async def main(host):
    cn = await Connection(host, user="sys", password="exasol")
    res = await cn.execute("SELECT 3, 'hi' UNION SELECT 5, 'hello'")
    data = await(res.fetchall())
    print(data)
    await res.close()
    await cn.close()
    
    # or using context managers
    async with await Connection(
            host, user="sys", password="exasol") as cn:
        async with await cn.execute(
                "SELECT 3, 'hi' UNION SELECT 5, 'hello'") as res:
            async for row in res: 
                print(row)


if __name__ == "__main__":
    asyncio.run(main(<hostname_or_ip>))
```

This will print

```
[(5, 'hello'), (3, 'hi')]
(5, 'hello')
(3, 'hi')
```

# value conversions

Exasyncio will try to convert some values to the logical Python counterpart

Exasol type | Python type
----------- | -----------
DECIMAL with scale 0 | int
DECIMAL with scale > 0 | decimal.Decimal
DATE | datetime.date
TIMESTAMP | datetime.datetime without tzinfo
TIMESTAMP WITH LOCAL TIME ZONE | datetime.datetime with tzinfo
HASHTYPE with '-' | uuid.UUID
HASHTYPE without '-' | bytes

The following types are already converted by the JSON protocol

Exasol type | Python type
----------- | -----------
DOUBLE | float
CHAR, VARCHAR | str
BOOLEAN | bool

Not converted (yet) are INTERVAL and GEOMETRY values.

# todo

* Proper timeout handling
* More type conversions
* Proper documentation
* Cancellation of statements
* Parallel execution
* And much more...

