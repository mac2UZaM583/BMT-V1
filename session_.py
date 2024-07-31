from pybit.unified_trading import HTTP
from settings_ import files_content

session = HTTP(
    demo=True,
    api_key=files_content['API_EXCHANGE'],
    api_secret=files_content['API_2_EXCHANGE']
)