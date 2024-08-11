from session_ import session
from settings_ import files_content

import numpy as np
import asyncio
from pprint import pprint
import time

async def g_densities():
    from set import limit_list_check

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
    
    klines = np.array(await asyncio.gather(*tuple(
        asyncio.create_task(asyncio.to_thread(
            lambda symbol=symbol: np.array((
                symbol,
                str(float(session.get_kline(
                    category='spot', 
                    symbol=symbol, 
                    interval='30', 
                    limit=1
                )['result']['list'][0][5]) * float(last_price))
            ))
        ))
        for symbol, last_price in symbols
    )))
    data_clean_2_volumes = np.float32(klines[:, 1])
    symbols = klines[:, 0][np.where(
        (data_clean_2_volumes > 10_000) & 
        (data_clean_2_volumes < 50_000)
    )]

    # DENSITIES
    orderbook = await asyncio.gather(*[
        asyncio.create_task(asyncio.to_thread(
            lambda symbol=symbol: session.get_orderbook(
                category='spot', 
                symbol=symbol, 
                limit=50
            )['result']
        ))
        for symbol in symbols
    ])

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
            symbol != limit_list_check(symbol.rstrip('USDT').rstrip('USDC')) and
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
            instruments_info['lotSizeFilter']['basePrecision'],
            instruments_info['priceFilter']['tickSize']
        )
        def sub(value):
            for index, el in enumerate(value):
                if el == '.':
                    return len(value[index+1:])
            return 0
                
        '''SET ⭣
        '''
        return {
            symbol: 
            (
                tple,
                tuple(map(lambda v: sub(v), tple))
            )
        }
    
    return {
        k: v for dct in 
        await asyncio.gather(*[
            asyncio.create_task(g_round_qty(symbol))
            for symbol in symbols
        ]) 
        for k, v in dct.items()
    }

async def g_open_orders(round_qtys):
    from set import limit_list_check

    async def g_wallet_balance():
        data = session.get_wallet_balance(
            accountType=files_content['ACCOUNT_TYPE']
        )['result']['list'][0]['coin']
        return {
            dct['coin']: float(dct['availableToWithdraw'])
            for dct in data
        }

    open_orders, balance = await asyncio.gather(*tuple(map(
        asyncio.create_task,
        (
            asyncio.to_thread(
                lambda: session.get_open_orders(
                    category='spot'
                )['result']['list']
            ),
            g_wallet_balance()
        )
    )))

    '''SET ⭣
    '''
    opened = {}
    non_existent = {}
    for symbol in round_qtys:
        lst = None
        for side in ('Sell', 'Buy'):
            lst = tuple(filter(
                lambda v: (
                    v['side'] == side and 
                    v['orderType'] == 'Limit' and 
                    v['symbol'] == symbol
                ),
                open_orders
            ))
            if lst:
                break
        
        coin = symbol.rstrip('USDT').rstrip('USDC')
        coin_balance = next(
            (
                balance[coin_] 
                for coin_ in balance 
                if coin_ != limit_list_check(coin_) and (
                    coin_ == coin and
                    balance[coin_] / int(files_content['ORDER_DIVIDER']) >= float(round_qtys[symbol][0][0])
                )
            ), 
            0
        )
        if lst:
            opened[symbol] = lst
        if len(lst) != int(files_content['ORDER_DIVIDER']):
            non_existent[symbol] = coin_balance
    return opened, non_existent, balance['USDT']

async def g_data_f(
    densities, 
    round_qtys, 
    opened,
    non_existent,
    balance_usdt
):
    async def g_data_fcc(
            density_tple, 
            symbol, 
            last_price,
            round_qty,
            round_price,
            round_price_float,
            non_existent,
            balance_usdt,
            order_divider
    ):
        from set import s_round
        print(symbol)
        # BUY
        side = 'Buy'
        side_density_price = density_tple[1]
        order_type = 'Limit'
        qty = s_round((balance_usdt / len(non_existent) / order_divider) / last_price, round_qty)
        price = lambda i: s_round(side_density_price + round_price_float * (i + 1), round_price)

        # SELL
        if non_existent[symbol]:
            side = 'Sell' 
            round_price_float = -round_price_float
            side_density_price = density_tple[0]
            qty = s_round(non_existent[symbol] / order_divider, round_qty)
            if density_tple[0] <  last_price:
                order_type = 'Market' 
                qty = s_round(non_existent[symbol], round_qty)
        
        '''SET ⭣
        '''
        if order_type == 'Limit':
            for i in range(order_divider):
                open.append(np.array(
                    (symbol, price(i), qty, side, order_type), 
                    dtype=np.str_
                ))
        else:
            open.append((symbol, None, qty, side, order_type))

    global cancel, open
    cancel = []
    open = []
    cancel = tuple(
        (symbol, v['orderId'])
        for symbol, value in opened.items() for v in value
        if len(value) != int(files_content['ORDER_DIVIDER'])
    )
    # pprint(opened)
    await asyncio.gather(*[
        asyncio.create_task(g_data_fcc(
            tple, 
            symbol, 
            float(session.get_tickers(
                category='spot', 
                symbol=symbol
            )['result']['list'][0]['lastPrice']),
            round_qtys[symbol][1][1],
            round_qtys[symbol][1][2],    
            float(round_qtys[symbol][0][2]),
            non_existent,
            balance_usdt,
            int(files_content['ORDER_DIVIDER'])
        ))
        for symbol, tple in densities.items()
        if not opened.get(symbol)
    ]) 
    return cancel, open

if __name__ == '__main__':
    pass