import pandas as pd


def macd(close, ema_1=12, ema_2=26, ema_3=9) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    MACD   = ema_1 - ema_2
    Signal = EMA(MACD)
    signal buy:  MACD cross Signal down->up
    signal sell: MACD cross Signal up->down
    :param close: close data
    :param ema_1: ema short period
    :param ema_2: ema long period
    :param ema_3: ema for signal line
    :return: tuple of macd and macd_signal
    """
    data_ema_1 = close.ewm(span=ema_1, adjust=False).mean()
    data_ema_2 = close.ewm(span=ema_2, adjust=False).mean()
    macd = data_ema_1 - data_ema_2
    macd_signal = macd.ewm(span=ema_3, adjust=False).mean()
    return macd, macd_signal


def rsi(close, days=14):
    """

    :param close: close data
    :param days: count of days back to calculate RSI
    :return: rsi
    """
    delta = close.diff(1)
    positive = delta.copy()
    negative = delta.copy()
    positive[positive < 0] = 0
    negative[negative > 0] = 0
    average_gain = positive.rolling(window=days).mean()
    average_loss = abs(negative.rolling(window=days).mean())
    relative_strength = average_gain / average_loss
    rsi = 100.0 - (100.0 / (1.0 + relative_strength))
    return rsi
