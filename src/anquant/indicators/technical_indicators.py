# src/anquant/indicators/technical_indicators.py
import pandas as pd
from loguru import logger


class TechnicalIndicators:
    """A utility class for calculating technical indicators."""

    @staticmethod
    def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI for the given price series using Wilder's smoothing method."""
        delta = prices.diff()

        # Separate gains and losses
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # Initial average gain/loss using simple moving average for the first period
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()

        # Use Wilder's smoothing method for subsequent values
        for i in range(period, len(prices)):
            if pd.notnull(avg_gain.iloc[i-1]) and pd.notnull(avg_loss.iloc[i-1]):
                avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period - 1) + gain.iloc[i]) / period
                avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period - 1) + loss.iloc[i]) / period

        # Calculate RS and RSI
        rs = avg_gain / avg_loss
        rs = rs.replace([float('inf'), -float('inf')], 0)
        rsi = 100 - (100 / (1 + rs))

        # Log intermediate steps for debugging
        logger.debug(f"RSI calculation for period {period}:")
        logger.debug(f"Average Gain (last): {avg_gain.iloc[-1] if len(avg_gain) > 0 else 'N/A'}")
        logger.debug(f"Average Loss (last): {avg_loss.iloc[-1] if len(avg_loss) > 0 else 'N/A'}")
        logger.debug(f"RS (last): {rs.iloc[-1] if len(rs) > 0 else 'N/A'}")

        return rsi

    @staticmethod
    def calculate_macd(data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        ema_fast = data.ewm(span=fast, adjust=False).mean()
        ema_slow = data.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal, adjust=False).mean()
        histogram = macd - signal_line
        return macd, signal_line, histogram

    @staticmethod
    def calculate_supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0) -> pd.Series:
        """Calculate Supertrend indicator."""
        hl2 = (df["high"] + df["low"]) / 2
        atr = (df["high"] - df["low"]).rolling(window=period).mean()
        upper_band = hl2 + (multiplier * atr)
        lower_band = hl2 - (multiplier * atr)

        supertrend = pd.Series(index=df.index, dtype=float)
        trend = pd.Series(index=df.index, dtype=bool)

        supertrend.iloc[0] = df["close"].iloc[0]
        trend.iloc[0] = True

        for i in range(1, len(df)):
            if trend.iloc[i-1]:
                supertrend.iloc[i] = lower_band.iloc[i]
                if df["close"].iloc[i] < supertrend.iloc[i]:
                    trend.iloc[i] = False
                    supertrend.iloc[i] = upper_band.iloc[i]
                else:
                    trend.iloc[i] = True
            else:
                supertrend.iloc[i] = upper_band.iloc[i]
                if df["close"].iloc[i] > supertrend.iloc[i]:
                    trend.iloc[i] = True
                    supertrend.iloc[i] = lower_band.iloc[i]
                else:
                    trend.iloc[i] = False

        return supertrend