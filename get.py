from session_ import session
from settings_ import files_content
import numpy as np
from numpy.typing import NDArray
import asyncio
from re import sub
from pprint import pprint

async def g_symbols():
    data_clean_1= np.array(tuple(
        value['symbol']
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
            lambda value=value: np.array((
                value,
                session.get_kline(
                    category='spot', 
                    symbol=value, 
                    interval='30', 
                    limit=1
                )['result']['list'][0][5]
            ))
        )
        for value in data_clean_1
    ]
    data_clean_2 = np.array(await asyncio.gather(*tasks))

    '''SET ⭣
    '''
    data_clean_2_volumes = np.float32(data_clean_2[:, 1])
    return data_clean_2[:, 0][np.where(
        (data_clean_2_volumes > 5_000) & 
        (data_clean_2_volumes < 40_000)
    )]

pprint(asyncio.run(g_symbols()))

async def g_orderbook(symbols):
    tasks = [
        asyncio.to_thread(
            lambda symbol=symbol: session.get_orderbook(
                category='spot', 
                symbol=symbol, 
                limit=30
            )['result']
        )
        for symbol in symbols
    ]
    return await asyncio.gather(*tasks)

def g_densities(data):
    def g_density(value, sides):
        return (
            np.str_(value['s']),
            np.array(tuple(
                (
                    lambda v: (
                        lambda v_1: np.min(v_1[:, 0]) if side == 'a' else np.max(v_1[:, 0])
                        )(v[np.argmax(np.diff(v[:, 1])):])
                )(np.array(sorted(np.float32(value[side]), key=lambda x: x[1], reverse=True if side == 'b' else False)))
                for side in sides
        )))
    def g_density_filtered(data):
        v_1 = data[1][0]
        v_2 = data[1][1]
        return (
            data[0],
            np.array((
                (v_1 / (v_2 / 100)) - 100, 
                v_1, 
                v_2
            ))
        )
    data = tuple(
        g_density(value, ('a', 'b'))
        for value in data
    )
    symbols, values = zip(*(
        g_density_filtered(value)
        for value in data
    ))
    values = np.array(values)
    v_indeces = values[:, 0]
    indeces = np.where((v_indeces >= 4) & (v_indeces <= 7))
    return (
        np.array(symbols)[indeces[0]], 
        values[:, [1, 2]][indeces]
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