from session_ import session
from settings_ import files_content

import numpy as np
import asyncio
from re import sub
from pprint import pprint
import time

async def g_densities():
    # SYMBOLS
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
    
    tasks = tuple(
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
    )
    klines = np.array(await asyncio.gather(*tasks))
    data_clean_2_volumes = np.float32(klines[:, 1])
    symbols = klines[:, 0][np.where(
        (data_clean_2_volumes > 10_000) & 
        (data_clean_2_volumes < 50_000)
    )]

    # DENSITIES
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

    densities = {}
    for value in orderbook:
        a = np.array(value['a'], dtype=np.float32)
        b = np.array(value['b'], dtype=np.float32)
        a_diff = a[np.argmax(a[:, 1])]
        b_diff = b[np.argmax(b[:, 1])]
        a_diff_i_0 = a_diff[0]
        b_diff_i_0 = b_diff[0]
        symbol = np.str_(value['s'])

        if (
            len(densities) < float(files_content['DENSITY_QTY_LIMIT']) and
            symbol != next((symbol_if for symbol_if in files_content['LIMIT_LIST'].split(' ') if symbol_if == symbol), None) and
            a_diff[1] < b_diff[1] and
            (
                a_diff_i_0 / b_diff_i_0 >= 1.03 and
                a_diff_i_0 / b_diff_i_0 <= 1.07
            )
        ):
            densities[symbol] = np.array((a_diff_i_0, b_diff_i_0))
    return densities

async def g_round_qtys(symbols):
    async def g_round_qty(symbol):
        instruments_info = session.get_instruments_info(
            category='spot',
            symbol=symbol
        )['result']['list'][0]
        tple = (
            instruments_info['lotSizeFilter']['minOrderQty'],
            instruments_info['priceFilter']['tickSize']
        )

        '''SET ⭣
        '''
        return {
            symbol: 
            (
                tple,
                tuple(map(lambda v: len(sub(r'^.*?\.', '', v)), tple))
            )
        }
    
    tasks = [
        g_round_qty(symbol)
        for symbol in symbols
    ]
    return {k: v for dct in await asyncio.gather(*tasks) for k, v in dct.items()}

async def g_validate_open_orders(symbols):
    data = session.get_open_orders(category='spot')['result']['list']
    
    dct = {}
    for symbol in symbols:
        lst = []
        for side in ('Sell', 'Buy'):
            result = tuple(filter(
                lambda v: (
                    v['side'] == side and 
                    v['orderType'] == 'Limit' and 
                    v['symbol'] == symbol
                ),
                data
            ))
            if result:
                lst.append(result)
        dct[symbol] = (
            lst,
            tuple(filter(lambda v: len(v) != 4, lst))
        )
    return dct

def cancel_append(dct):
    cancel_ = tuple(
        (symbol, v_['orderId'])
        for symbol, value in dct.items() for v in value[1] for v_ in v
    )
    cancel.append(cancel_) if cancel_ else None

async def g_wallet_balance():
    data = session.get_wallet_balance(
        accountType=files_content['ACCOUNT_TYPE']
    )['result']['list'][0]['coin']
    return {
        dct['coin']: float(dct['availableToWithdraw'])
        for dct in data
    }

async def g_non_changing_data():
    densities = await g_densities()
    return (
        densities,
        await g_round_qtys(densities)
    )

async def g_changing_data(densities):
    tasks = (
        g_validate_open_orders(densities),
        g_wallet_balance()
    )
    return await asyncio.gather(*tasks)

async def g_data_f(
        densities, 
        round_qtys, 
        orders_open, 
        wallet_balance
    ):
    async def g_data_fcc(
            symbol, 
            density_tple, 
            round_qtys,
            round_qty,
            round_price,
            round_price_float,
            orders_open, 
            wallet_balance
        ):
        def s_round(value, round):
            lst = str(value).split('.')
            lst[1] = lst[1][:round]
            return '.'.join(lst)

        # BUY
        last_price = float(session.get_tickers(
            category='spot', 
            symbol=symbol
        )['result']['list'][0]['lastPrice'])
        side = 'Buy'
        order_type = 'Limit'
        qty = s_round((wallet_balance['USDT'] / len(round_qtys) / 4) / last_price, round_qty)
        side_density_price = density_tple[1]
        price = lambda i: s_round(side_density_price + round_price_float * (i + 1), round_price)

        # SELL
        if next((
            wallet_balance[symbol_if] 
            for symbol_if in wallet_balance 
            if symbol_if == symbol.rstrip('USDT').rstrip('USDC')
        ), 0) >= float(round_qtys[symbol][0][0]):
            side = 'Sell' 
            qty = s_round(wallet_balance[symbol] / 4, round_qty)
            round_price_float = -round_price_float
            side_density_price = density_tple[0]
            if density_tple[0] <  last_price:
                order_type = 'Market' 
                qty = s_round(wallet_balance[symbol], round_qty)
        
        '''SET ⭣
        '''
        if order_type == 'Limit':
            if not orders_open[symbol][1]:
                for i in range(4):
                    open.append(np.array(
                        (symbol, price(i), qty, side, order_type), 
                        dtype=np.str_
                    ))
            else:
                for i in range(len(orders_open[symbol][1][0])):
                    open.append(np.array(
                        (symbol, price(i), qty, side, order_type), 
                        dtype=np.str_
                    ))
        else:
            open.append((symbol, None, qty, side, order_type))

    global cancel, open
    cancel = []
    open = []
    tasks = [
        g_data_fcc(
            symbol, 
            tple, 
            round_qtys, 
            *round_qtys[symbol][1],
            float(round_qtys[symbol][0][1]),
            orders_open, 
            wallet_balance
        )
        for symbol, tple in densities.items()
        if not orders_open[symbol][0]
    ]
    cancel_append(orders_open)
    await asyncio.gather(*tasks)
    return cancel, open

if __name__ == '__main__':
    pass