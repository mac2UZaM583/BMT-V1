from pybit.unified_trading import HTTP
import time
from numba import jit, njit, prange
import asyncio
import numpy as np
import pickle
from numpy.typing import NDArray
from settings__ import files_content
from re import sub
from pprint import pprint

session = HTTP(
    demo=True,
    api_key=files_content['API_EXCHANGE'],
    api_secret=files_content['API_2_EXCHANGE']
)

def s_time_meter(func):
    def wrapper(*args):
        start = time.perf_counter()
        result = func(*args)
        print(time.perf_counter() - start)
        return result
    return wrapper

'''
1 ⭣
'''
def g_symbols() -> NDArray:
    return np.array(tuple(
        value['symbol']
        for value in session.get_tickers(category='spot')['result']['list']
        if (lambda symbol: 'USDT' in symbol and 'USDC' not in symbol)(value['symbol'])
    ), dtype=np.str_)

async def g_klines(data: NDArray) -> list[dict]:
    tasks = [
        asyncio.to_thread(
            lambda symbol=symbol: session.get_kline(
                category='spot', 
                symbol=symbol, 
                interval='30', 
                limit=1
            )['result']
        )
        for symbol in data
    ]
    return await asyncio.gather(*tasks)

def g_data_filtered(data: list[dict]) -> NDArray:
    symbols, klines = zip(*(
        (value['symbol'], value['list'][0])
        for value in np.array(data)
    ))
    return np.array(symbols, dtype=np.str_), np.float32(klines)

def g_symbols_filtered(data: tuple) -> NDArray:
    return np.array(tuple(
        symbol
        for symbol, klines in zip(*data)
        if (lambda v: v >= 5_000 and v <= 40_000)(klines[5])
    ), dtype=np.str_)

async def g_orderbook(symbols: NDArray) -> list[dict]:
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

def g_densities(data: list[dict]) -> tuple[str, tuple]:
    def get_density(value: dict[str, list], sides: tuple[str]):
        return (
            np.str_(value['s']),
            np.array(tuple(
                (lambda v: (lambda v_1: np.min(v_1[:, 0]) if side == 'a' else np.max(v_1[:, 0]))(
                    v[np.argmax(np.diff(v[:, 1])):]))(
                        np.array(sorted(np.float32(value[side]), key=lambda x: x[1], reverse=True if side == 'b' else False))
                    )
                for side in sides
        )))
    sides = ('a', 'b')
    return tuple(
        get_density(value, sides)
        for value in data
    )

def g_densities_filtered(data: tuple) -> tuple[NDArray]:
    def g_change_percent(data: tuple[str, tuple]) -> tuple[str, NDArray]:
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
    symbols, values = zip(*(
        g_change_percent(value)
        for value in data
    ))
    values = np.array(values)
    v_indeces = values[:, 0]
    indeces = np.where((v_indeces >= 4) & (v_indeces <= 7))
    return (
        np.array(symbols)[indeces[0]], 
        values[:, [1, 2]][indeces]
    )

'''
2 ⭣
'''
async def g_symbols_data_filtered(data: tuple[NDArray]) -> tuple[NDArray]:
    # (((symbols), (buy), ((price_buy, price_sell),) (qty)), 
    # ((symbols), (orders_id)))
    
    filtered_limit_orders = np.array([
        (order['symbol'], order['side'])
        for order in session.get_open_orders(category='spot', settleCoin='USDT')['result']['list']
        if order['symbol'] in data[0] and order['orderType'] == 'Limit'
    ], dtype=[('symbol', 'U10'), ('side', 'U10')])
    return tuple(
        (symbol, np.array(filtered_limit_orders['side'][filtered_limit_orders['symbol'] == symbol])) 
        for symbol in np.unique(filtered_limit_orders['symbol'])
    )

async def g_round_qty(symbol: str) -> NDArray:
    data = session.get_instruments_info(
        category='spot',
        symbol=symbol
    )['result']['list'][0]
    return np.array(tuple(map(
            lambda v: len(sub(r'^.*?\.', '', v)), 
            (data['lotSizeFilter']['minOrderQty'], data['priceFilter']['tickSize'])
    )))

async def g_balance() -> float:
    return np.float16(session.get_wallet_balance(
        accountType='UNIFIED', 
        coin='USDT'
    )['result']['list'][0]['coin'][0]['availableToWithdraw'])

async def g_data_for_place_orders() -> list:
    '''
    (array(symbols), array(sides), )
    
    '''
    
    tasks = [
        g_round_qty(symbol)
        for symbol in g_symbols_data_filtered(g_densities_filtered(g_densities(
            asyncio.run(g_orderbook(g_symbols_filtered(g_data_filtered(
                asyncio.run(g_klines(g_symbols()))
            ))))
        )))
    ].extend(g_balance())
    return await asyncio.gather(*tasks)

async def s_cancel_orders():
    tasks = [
        session.cancel_order(
            category='spot',
            symbol=symbol,
            orderId=order_id
        )
        # for symbol
    ]
    await asyncio.gather(*tasks)

async def s_places_orders(data):
    '''
    if not limit_orders: place_order_buy (tpsl)
    if orders_buy < 4: cancel_order
    if orders_sell < 4 place_order_sell
    if mark_price < density_price: place_order_sell

    '''
    
    tasks = [
        session.place_order(
            category='spot',
            symbol=symbol,
            orderType='Limit',
            price=price,
            qty=qty,
            side=side,
            marketUnit='baseCoin'

        )
        # for symbol
    ]
    await asyncio.gather(*tasks)



with open('data.pkl', 'rb') as f:
    data = pickle.load(f)
pprint(
    g_symbols_data_filtered(
        g_densities_filtered(g_densities(data))
    )
)



# with open('data.pkl', 'wb') as f:
#     pickle.dump(
#         asyncio.run(g_orderbook(g_symbols_filtered(g_data_filtered(
#             asyncio.run(g_klines(g_symbols()))
#         )))),
#         f
#     )

