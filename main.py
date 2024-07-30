from pybit.unified_trading import HTTP
import time
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
G ⭣
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

def g_symbols_filtered(data: list[dict]) -> NDArray:
    symbols, klines = zip(*(
        (value['symbol'], value['list'][0])
        for value in np.array(data)
    ))
    return np.array(tuple(
        symbol
        for symbol, klines in (np.array(symbols, dtype=np.str_), np.float32(klines))
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

'''
S ⭣
'''
@s_time_meter
def s_packing_data(data) -> list:
    pprint(data)
    # нужно определить сколько buy и sell в каждой из монеты
    

    # (((symbols), (buy), ((price_buy, price_sell),) (qty)), 
    # ((symbols), (orders_id)))
    '''
    if not limit_orders: place_order_buy
    if orders_buy < 4: cancel_order & place_order_sell_limit
    if orders_sell < 4 place_order_sell_market
    if mark_price < density_price: place_order_sell

    '''
with open('data_1.pkl', 'rb') as f:
    data = pickle.load(f)
s_packing_data(data)

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


# session.place_order(
#     category='spot',
#     symbol='BLOCKUSDT',
#     orderType='Limit',
#     price='0.055',
#     qty='200',
#     side='Buy',
#     spotLoss='0.05'
# )


# with open('data.pkl', 'wb') as f:
#     pickle.dump(
#         asyncio.run(g_orderbook(g_symbols_filtered(g_data_filtered(
#             asyncio.run(g_klines(g_symbols()))
#         )))),
#         f
#     )

