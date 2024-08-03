from session_ import session
from settings_ import files_content
from set import s_time_meter

import numpy as np
import asyncio
from re import sub
from pprint import pprint

async def g_symbols():
    '''
    RETURNS:
    NDarray[str]:: the list of filtered symbols

    '''
    symbols = np.array(tuple(
        (value['symbol'], value['lastPrice'])
        for value in session.get_tickers(
            category='spot'
        )['result']['list']
        if (
            lambda symbol: 'USDT' in symbol and 
            'USDC' not in symbol
        )(value['symbol'])
    ), dtype=np.str_)
    
    tasks = [
        asyncio.to_thread(
            lambda symbol=symbol: np.array((
                symbol,
                str(float(session.get_kline(
                    category='spot', 
                    symbol=symbol, 
                    interval='30', 
                    limit=1
                )['result']['list'][0][5]) * float(last_price))
            ))
        )
        for symbol, last_price in symbols
    ]
    klines = np.array(await asyncio.gather(*tasks))

    '''SET ⭣
    '''
    data_clean_2_volumes = np.float32(klines[:, 1])
    return klines[:, 0][np.where(
        (data_clean_2_volumes > 10_000) & 
        (data_clean_2_volumes < 50_000)
    )]

@s_time_meter
async def g_densities(symbols):
    '''
    ARGS:
    data (NDarray[str]):: numpy array with symbols
    
    RETURNS:
    NDarray[tuple]:
    tuple[str, NDarray[float]]:: {
        tuple with the orderbook symbol, 
        NDarray with the density price 'a' and 'b'
    }
     
    '''
    tasks = [
        asyncio.to_thread(
            lambda symbol=symbol: session.get_orderbook(
                category='spot', 
                symbol=symbol, 
                limit=50
            )['result']
        )
        for symbol in symbols
    ]
    orderbook = await asyncio.gather(*tasks)

    '''SET ⭣
    '''
    densities = []
    for value in orderbook:
        a = np.array(value['a'], dtype=np.float32)
        b = np.array(value['b'], dtype=np.float32)
        a_diff = a[np.argmax(a[:, 1])]
        b_diff = b[np.argmax(b[:, 1])]
        a_diff_i_0 = a_diff[0]
        b_diff_i_0 = b_diff[0]
        
        if (
            a_diff[1] < b_diff[1] and
            (
                a_diff_i_0 / b_diff_i_0 >= 1.03 and
                a_diff_i_0 / b_diff_i_0 <= 1.07
            )
        ):
            densities.append((
                np.str_(value['s']),
                np.array((a_diff_i_0, b_diff_i_0))
            ))
    return tuple(densities)

pprint(
    asyncio.run(g_densities(asyncio.run(g_symbols())))
)

async def g_round_qtys(symbols):
    async def g_round_qty(symbol):
        data = session.get_instruments_info(
            category='spot',
            symbol=symbol
        )['result']['list'][0]
        return np.array(tuple(map(
            lambda v: len(sub(r'^.*?\.', '', v)), 
            (data['lotSizeFilter']['minOrderQty'], data['priceFilter']['tickSize'])
        )))
    tasks = [
        g_round_qty(symbol)
        for symbol in symbols
    ]
    return await asyncio.gather(*tasks)

async def g_data(data):
    tasks = [
        g_round_qtys(data[0]),
        asyncio.to_thread(
            lambda: session.get_open_orders(
                category='spot'
            )['result']['list']
        ),
        asyncio.to_thread(
            lambda: session.get_wallet_balance(
                accountType=files_content['ACCOUNT_TYPE']
            )['result']['list'][0]['coin']
        )
    ]
    return await (
        asyncio.gather(*tasks),
        data
    )