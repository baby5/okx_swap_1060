# coding: utf-8

import asyncio
import io
import sys
from collections import defaultdict
from datetime import datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import aiohttp
import aiosmtplib
import mplfinance as mpf
import pandas as pd
from loguru import logger

from sensitive_config import EMAIL_CONF, PROXY

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.level("RED", no=38, color="<red>")
logger.level("GREEN", no=38, color="<green>")

HOST = "https://www.okx.com"

# https://www.okx.com/docs-v5/zh/#order-book-trading-market-data-get-tickers
API_TICKERS = f"{HOST}/api/v5/market/tickers"

# https://www.okx.com/docs-v5/zh/?shell#order-book-trading-market-data-get-candlesticks
API_CANDLES = f"{HOST}/api/v5/market/candles"

SLEEP_TIME = 60 * 5
CANDLE_BAR = "1H"

CACHE = defaultdict(set)


async def get_inst_id_list(session):
    params = {"instType": "SWAP"}
    async with session.get(API_TICKERS, params=params, proxy=PROXY) as response:
        result = await response.json()

    inst_id_list = []
    for data in result["data"]:
        inst_id = data["instId"]
        # 只要 USDT
        if "USDT" in inst_id:
            inst_id_list.append(inst_id)

    logger.info(f"Get inst_id_list success: {len(inst_id_list)}")

    return inst_id_list


async def get_candle_df(inst_id, limit, session, bar=CANDLE_BAR):
    params = {"instId": inst_id, "bar": bar, "limit": limit}

    while True:
        try:
            async with session.get(API_CANDLES, params=params, proxy=PROXY) as response:
                result = await response.json()
        except aiohttp.ClientError as e:
            await asyncio.sleep(5)

            continue

        if result["code"] == "0":
            break
        else:
            msg = result["msg"]
            code = result["code"]
            logger.debug(
                f"API_CANDLES failed. msg: {msg} code: {code} params: {params}. to retry..."
            )

            await asyncio.sleep(5)

    columns = ["open", "high", "low", "close", "volume"]
    index = []
    new_data = []

    for data in reversed(result["data"]):
        dt = datetime.fromtimestamp(int(data[0]) / 1000)
        index.append(pd.to_datetime(dt))

        new_data.append(
            [
                float(data[1]),
                float(data[2]),
                float(data[3]),
                float(data[4]),
                float(data[5]),
            ]
        )

    return pd.DataFrame(new_data, columns=columns, index=index)


async def email(inst_id, act, k_img):
    msg = MIMEMultipart()
    msg["From"] = EMAIL_CONF["from"]
    msg["To"] = ",".join(EMAIL_CONF["to"])
    msg["Subject"] = f"{act} {inst_id}"
    msg.attach(
        MIMEText(
            f"{datetime.now()}\nhttps://www.okx.com/cn/trade-swap/{inst_id}#layout=-4"
        )
    )
    msg.attach(MIMEImage(k_img.read()))

    email_client = aiosmtplib.SMTP(
        hostname=EMAIL_CONF["host"],
        port=EMAIL_CONF["port"],
        username=EMAIL_CONF["user"],
        password=EMAIL_CONF["password"],
    )

    async with email_client:
        await email_client.send_message(msg)


def is_dup(inst_id, act):
    """
    这个去重方案适配的是 1 小时 K 线的，如果要适配多种 K 线，需要修改下
    """

    key = datetime.now().strftime("%Y%m%d%H")
    value = f"{act}{inst_id}"

    if key not in CACHE:
        CACHE.clear()  # key是时间序的，更新了，清空旧的缓存，防止内存泄漏

    if value in CACHE[key]:
        return True
    else:
        CACHE[key].add(value)
        return False


def get_k_img(df):
    apdict = mpf.make_addplot(df[["ema60", "ema10"]])

    buf = io.BytesIO()
    mpf.plot(df, type="candle", volume=True, addplot=apdict, savefig=buf)
    buf.seek(0)

    return buf


async def judge(df, inst_id):
    # 计算 EMA
    df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
    df["ema60"] = df["close"].ewm(span=60, adjust=False).mean()

    # 多空判断
    last1 = df.iloc[-1]
    last2 = df.iloc[-2]

    logger.info(
        f"{inst_id} last2: ema60 {last2.ema60} ema10 {last2.ema10} last1: ema60 {last1.ema60} ema10 {last1.ema10}"
    )

    if (last2.ema60 >= last2.ema10) and (last1.ema60 <= last1.ema10):  # 10 上穿 60
        act = "Call"
        if not is_dup(inst_id, act):
            logger.log("GREEN", f"!!! - {act} {inst_id} - !!!")
            k_img = get_k_img(df)
            await email(inst_id, act, k_img)

    elif (last2.ema60 <= last2.ema10) and (last1.ema60 >= last1.ema10):  # 10 下穿 60
        act = "Put"
        if not is_dup(inst_id, act):
            logger.log("RED", f"!!! - {act} {inst_id} - !!!")
            k_img = get_k_img(df)
            await email(inst_id, act, k_img)

    else:
        logger.info(f"Nothing happened {inst_id}, to sleep...")

    await asyncio.sleep(SLEEP_TIME)


async def monitor(inst_id, session):
    logger.info(f"Start monitor {inst_id}")

    df = await get_candle_df(inst_id, 300, session)
    await judge(df, inst_id)

    # 获取新的数据
    while True:
        last_index = df.index[-1]

        new_df = await get_candle_df(inst_id, "2", session)
        new_index = new_df.index[-1]

        if str(new_index) == str(last_index):  # 没有新 K 线
            df.loc[last_index] = new_df.loc[new_index]  # 更新数据
        else:  # 有新 K 线
            df.loc[last_index] = new_df.iloc[-2]  # 更新旧 K 线数据
            df.loc[new_index] = new_df.loc[new_index]  # 添加新 K 线

            df.drop(df.index[0], inplace=True)  # 删除第一条 K 线，不增加内存

        await judge(df, inst_id)


async def woker(inst_id, session):
    try:
        await monitor(inst_id, session)
    except Exception as e:
        logger.exception(f"Error: {inst_id}", exc_info=e)


async def main():
    async with aiohttp.ClientSession() as session:
        inst_id_list = await get_inst_id_list(session)

        # inst_id = "BTC-USDT-SWAP"
        # task = asyncio.create_task(monitor(inst_id, session))
        # await task

        tasks = [woker(inst_id, session) for inst_id in inst_id_list]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
