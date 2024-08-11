from get import (
    g_data_f,
    g_densities,
    g_round_qtys,
    g_open_orders
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



'''
получаем:
плотности,
раунды,
открытые ордера,
баланс юздт,

открытые ордера:: {
    false -> buy,
    false and balance -> sell,

    true and != 4 -> cancel

    
}








'''



async def main():
    while True:
        await s_pre_preparation()
        
        densities = await g_densities()
        round_qtys = await g_round_qtys(densities)
        start = time.time()
        print('cycle started')
        while time.time() - start < float(files_content['CYCLE_UPDATE']) and densities:
            data = await g_data_f(
                densities,
                round_qtys,
                *await g_open_orders(round_qtys)
            )
            await s_data(data)

if __name__ == '__main__':
    asyncio.run(main())
