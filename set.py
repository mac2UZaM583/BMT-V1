from session_ import session
from settings_ import files_content

import time
import asyncio
import traceback
from pprint import pprint

async def s_cancel_order(data):
    tasks = [
        asyncio.to_thread(
            session.cancel_order, 
            category='spot', 
            symbol=symbol, 
            orderId=order_id
        )
        for symbol, order_id in data
    ]
    return await asyncio.gather(*tasks)

async def s_place_orders(data):
    try:
        time.sleep(0.4)
        tasks = [
            asyncio.to_thread(
                session.place_order,
                category='spot',
                symbol=symbol,
                orderType=order_type,
                price=price,
                qty=qty,
                side=side,
                marketUnit='baseCoin'
            )
            for symbol, price, qty, side, order_type in data
        ]
        return await asyncio.gather(*tasks)
    except:
        traceback.print_exc()

async def s_data(data):
    return (
        await s_cancel_order(data[0]),
        await s_place_orders(data[1])
    )

async def s_pre_preparation():
    try:
        session.cancel_all_orders(category='spot')

        wallet_balance = {
            value['coin']: value['availableToWithdraw']
            for value in session.get_wallet_balance(
                accountType=files_content['ACCOUNT_TYPE']
            )['result']['list'][0]['coin']
        }
        tasks = [
            asyncio.to_thread(lambda symbol=symbol: {
                symbol:
                session.get_instruments_info(
                    category='spot', symbol=symbol + 'USDT'
                )['result']['list'][0]['lotSizeFilter']['minOrderQty']
            })
            for symbol in wallet_balance
            if symbol != next(
                (
                    symbol_if 
                    for symbol_if in files_content['LIMIT_LIST'].split(' ') 
                    if symbol_if.rstrip('USDT').rstrip('USDC') == symbol
                ), 
                None
            ) and
            symbol != 'USDT'
        ]
        instruments_info = {k: v for value in await asyncio.gather(*tasks) for k, v in value.items()}
        instruments_info_f = {
            symbol: (wallet_balance[symbol], len(instruments_info[symbol]))
            for symbol in instruments_info
            if float(wallet_balance[symbol]) >= float(instruments_info[symbol])
        }
        tasks = [
            asyncio.to_thread(session.place_order,
                category='spot',
                symbol=symbol + 'USDT',
                orderType='Market',
                qty=instruments_info_f[symbol][0][:instruments_info_f[symbol][1]],
                side='Sell',
                marketUnit='baseCoin'
            )
            for symbol in instruments_info_f
        ]
        await asyncio.gather(*tasks)
    except:
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(s_pre_preparation())
    pass
