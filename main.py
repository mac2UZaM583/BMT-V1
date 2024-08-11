from get import (
    g_data_f,
    g_densities,
    g_round_qtys,
    g_orders,
    g_wallet_balance
)
from set import (
    s_data,
    s_pre_preparation
)
from settings_ import files_content

import asyncio
import time
import traceback
from pprint import pprint

async def main():
    while True:
        await s_pre_preparation()
        
        densities = await g_densities()
        round_qtys = await g_round_qtys(densities)
        start = time.time()
        print('cycle started')
        while (
            time.time() - start < float(files_content['CYCLE_UPDATE']) and
            len(densities) == int(files_content['DENSITY_QTY_LIMIT'])
        ):
            data = await g_data_f(
                densities,
                round_qtys,
                *await g_orders(round_qtys),
                g_wallet_balance()
            )
            await s_data(data)

if __name__ == '__main__':
    asyncio.run(main())
