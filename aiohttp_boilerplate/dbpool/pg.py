import asyncpg
import re
import json
import ssl

from aiohttp_boilerplate.views import fixed_dump


# encoder/decoder is needed to work correctly with jsonb fields
def _encoder(value):
    val = bytes(fixed_dump(value).encode('utf-8'))
    val = b'\x01' + val
    return val


def _decoder(value):
    return json.loads(
        re.sub(r'(\n|\t|\x01)', '', value.decode('utf-8'))
    )


async def setup_connection(conn):

    await conn.set_type_codec(
        'jsonb',
        encoder=_encoder,
        decoder=_decoder,
        schema='pg_catalog',
        format='binary',
    )


async def create_connection(conf, loop=None):

    return await asyncpg.connect(
        **conf,
        loop=loop
    )

async def create_pool(conf, loop=None):

    ssl_object = ssl.create_default_context()
    ssl_object.check_hostname = False
    ssl_object.verify_mode = ssl.CERT_NONE

    return await asyncpg.create_pool(
        **conf,
        ssl=ssl_object,
        loop=loop,
        setup=setup_connection
    )
