from get import (
    g_data_f,
    g_changing_data,
    g_non_changing_data
)
from set import (
    s_data,
    s_pre_preparation
)
from settings_ import files_content

import asyncio
import time
from pprint import pprint
import traceback

async def main():
    while True:
        await s_pre_preparation()
        
        changing_data = await g_non_changing_data()
        start = time.time()
        while start - time.time() < float(files_content['CYCLE_UPDATE']):
            data = await g_data_f(
                *changing_data, *await g_changing_data(changing_data[0])
            )
            await s_data(data)
            pprint(data)

if __name__ == '__main__':
    asyncio.run(main())



