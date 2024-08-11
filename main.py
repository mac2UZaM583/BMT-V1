from get import (
    g_data_f,
    g_densities,
    g_round_qtys,
    g_non_opened_orders,
    g_wallet_balance,
    g_last_prices
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
        print('cycle started')
        await s_pre_preparation()

        densities = await g_densities()
        round_qtys = await g_round_qtys(densities)
        start = time.time()
        while time.time() - start < float(files_content['CYCLE_UPDATE']) and densities:
            print('cycle updated')
            last_prices = g_last_prices(round_qtys)
            balance = g_wallet_balance()
            await s_data(await g_data_f(
                densities,
                round_qtys,
                balance,
                last_prices,
                g_non_opened_orders(round_qtys, last_prices, densities, balance),
            ))
            for symbol_ in [   
                symbol
                for symbol, tple in densities.items()    
                if last_prices[symbol] < tple[1] or (
                    balance.get(symbol.rstrip('USDT').rstrip('USDT')) and 
                    balance[symbol.rstrip('USDT').rstrip('USDT')] / int(files_content['ORDER_DIVIDER']) < round_qtys[symbol][0][0]
                )
            ]:
                del densities[symbol_]
                del round_qtys[symbol_]
            pprint(len(densities))
            

if __name__ == '__main__':    
    asyncio.run(main())
