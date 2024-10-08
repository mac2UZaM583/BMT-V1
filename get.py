from session_ import session, session_
from settings_ import files_content

import numpy as np
import asyncio
from pprint import pprint

async def g_densities():
    from set import limit_list_check

    # SYMBOLS
    symbols = np.array(tuple(
        (value['symbol'], value['lastPrice'])
        for value in session_.get_tickers(
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
                str(float(session_.get_kline(
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
            lambda symbol=symbol: session_.get_orderbook(
                category='spot', 
                symbol=symbol, 
                limit=200
            )['result']
        ))
        for symbol in symbols
    ])

    densities = {}
    limit = int(files_content['DENSITY_QTY_LIMIT'])
    for value in orderbook:
        a = np.array(value['a'], dtype=np.float32)
        b = np.array(value['b'], dtype=np.float32)
        a_diff = a[np.argmax(a[:, 1])]
        b_diff = b[np.argmax(b[:, 1])]
        a_diff_i_0 = a_diff[0]
        b_diff_i_0 = b_diff[0]
        symbol = np.str_(value['s'])

        if (
            len(densities) < limit and
            symbol != limit_list_check(symbol) and
            a_diff[1] < b_diff[1] and
            (
                a_diff_i_0 / b_diff_i_0 >= float(files_content['DENSITY_PERCENT_THRESHOLD']) and
                a_diff_i_0 / b_diff_i_0 <= float(files_content['DENSITY_PERCENT_LIMIT'])
            )
        ):
            densities[symbol] = np.array((a_diff_i_0, b_diff_i_0))
    
    if len(densities) == limit:
        return densities
    return {}

async def g_round_qtys(symbols):
    def sub(value):
        for index, el in enumerate(value):
            if el == '.':
                return len(value[index+1:])
        return 0
    
    instruments_info = session_.get_instruments_info(
        category='spot',
    )['result']['list']

    '''SET ⭣
    '''
    return {
        value['symbol']: 
        tuple(map(lambda v: (float(v), sub(v)), (
            value['lotSizeFilter']['minOrderQty'],
            value['lotSizeFilter']['basePrecision'],
            value['priceFilter']['tickSize']
        )))
        for value in instruments_info
        if value['symbol'] in symbols
    }
    
def g_non_opened_orders(round_qtys, last_prices, densities, balance):
    open_orders = session.get_open_orders(
        category='spot', 
        limit=int(int(files_content['DENSITY_QTY_LIMIT'] * int(files_content['ORDER_DIVIDER'])))
    )['result']['list']

    '''SET ⭣
    '''
    non_opened = {}
    for symbol in round_qtys:
        lst = []
        for side in ('Sell', 'Buy'):
            filtered = tuple(filter(
                lambda v: (
                    v['symbol'] == symbol and
                    v['side'] == side and 
                    v['orderType'] == 'Limit'
                ),
                open_orders
            ))
            lst.extend(filtered)
        
        coin = symbol.rstrip('USDT').rstrip('USDC')
        if (
            len(lst) != int(files_content['ORDER_DIVIDER']) or 
            (
                last_prices[symbol] < densities[symbol][0] and
                balance.get(coin) and balance[coin] >= round_qtys[symbol][0][0]
            )
        ):
            non_opened[symbol] = lst
    return non_opened

def g_wallet_balance():
    return {
        dct['coin']: float(dct['availableToWithdraw'])
        for dct in session.get_wallet_balance(
            accountType=files_content['ACCOUNT_TYPE']
        )['result']['list'][0]['coin']
    }

def g_last_prices(symbols):
    return {
        value['symbol']:
        float(value['lastPrice'])
        for value in session.get_tickers(
            category='spot',
        )['result']['list']
        if value['symbol'] in symbols
    }

async def g_data_f(
    densities, 
    round_qtys, 
    wallet_balance,
    last_prices,
    non_opened
):
    async def g_data_fcc(
            symbol, 
            coin,
            density_tple, 
            order_divider,
            last_price,
            round_qty,
            round_price,
            round_qty_float,
            round_price_float,
            non_opened,
            wallet_balance
    ):
        from set import s_round

        # BUY
        side = 'Buy'
        side_density_price = density_tple[1]
        order_type = 'Limit'
        qty = s_round(((wallet_balance['USDT'] / len(non_opened)) / order_divider) / last_price, round_qty)
        price = lambda i: s_round(side_density_price + round_price_float * (i + 1), round_price)

        # SELL
        if wallet_balance.get(coin) and wallet_balance[coin] / order_divider >= round_qty_float:
            side = 'Sell' 
            round_price_float = -round_price_float
            side_density_price = density_tple[0]
            qty = s_round(wallet_balance[coin] / order_divider, round_qty)
            if last_prices[symbol] < density_tple[0]:
                order_type = 'Market' 
                qty = s_round(wallet_balance[coin], round_qty)
        
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
        for symbol, value in non_opened.items() for v in value
    )
    await asyncio.gather(*[
        asyncio.create_task(g_data_fcc(
            symbol, 
            symbol.rstrip('USDT').rstrip('USDC'),
            tple, 
            int(files_content['ORDER_DIVIDER']),
            last_prices[symbol],
            round_qtys[symbol][1][1],
            round_qtys[symbol][2][1],  
            round_qtys[symbol][0][0],
            round_qtys[symbol][2][0],
            non_opened,
            wallet_balance
        ))
        for symbol, tple in densities.items()
        if symbol in non_opened
    ]) 
    return cancel, open

if __name__ == '__main__':
    pprint(
        asyncio.run(g_round_qtys(('PIRATEUSDT',)))
    )