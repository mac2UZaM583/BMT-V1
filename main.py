import time
import asyncio
import numpy as np
import pickle
from re import sub
from settings_ import files_content
from pprint import pprint

def s_time_meter(func):
    def wrapper(*args):
        start = time.perf_counter()
        result = func(*args)
        print(time.perf_counter() - start)
        return result
    return wrapper


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

