from session_ import session
from get import g_round_qtys
from settings_ import files_content

import asyncio
import traceback
from pprint import pprint

def s_round(value, round):
    lst = str(f'{value:.{20}f}').split('.')
    if len(lst) > 1:
        lst[1] = lst[1][:round]
    if round > 0:
        return '.'.join(lst).rstrip('0')
    return ''.join(lst).rstrip('0')

limit_list_check = lambda coin: next((
    coin
    for coin_ in files_content['LIMIT_LIST'].split(' ')
    if coin_ == coin.rstrip('USDT').rstrip('USDC')
), None)

async def s_cancel_order(data):
    return await asyncio.gather(*[
        asyncio.create_task(asyncio.to_thread(
            session.cancel_order, 
            category='spot', 
            symbol=symbol, 
            orderId=order_id
        ))
        for symbol, order_id in data
    ])

async def s_place_orders(data):
    return await asyncio.gather(*[
        asyncio.create_task(asyncio.to_thread(
            session.place_order,
            category='spot',
            symbol=symbol,
            orderType=order_type,
            price=price,
            qty=qty,
            side=side,
            marketUnit='baseCoin'
        ))
        for symbol, price, qty, side, order_type in data
    ])

async def s_data(data):
    return (
        await s_cancel_order(data[0]),
        await s_place_orders(data[1])
    )

async def s_pre_preparation():
    try:
        session.cancel_all_orders(category='spot')

        wallet_balance = {
            value['coin'] + 'USDT': float(value['availableToWithdraw'])
            for value in session.get_wallet_balance(
                accountType=files_content['ACCOUNT_TYPE']
            )['result']['list'][0]['coin']
            if value['coin'] != limit_list_check(value['coin']) and
            value['coin'] != 'USDT' and
            value['coin'] != 'USDC'
        }
        instruments_info = await g_round_qtys(wallet_balance)
        await asyncio.gather(*[
            asyncio.to_thread(session.place_order,
                category='spot',
                symbol=symbol,
                orderType='Market',
                qty=s_round(wallet_balance[symbol], instruments_info[symbol][0][1]),
                side='Sell',
                marketUnit='baseCoin'
            )
            for symbol in instruments_info
            if wallet_balance[symbol] >= instruments_info[symbol][0][0]
        ])
    except:
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(s_pre_preparation())
    pass
