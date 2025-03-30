"""Data processor for stablecoin analytics."""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np


class StablecoinDataProcessor:
    """Processor for analyzing stablecoin data from DeFi Llama."""

    def __init__(self) -> None:
        """Initialize the data processor."""
        self.stablecoins_df = pd.DataFrame()
        self.chains_df = pd.DataFrame()
        # self.prices_df = pd.DataFrame()
        # self.yields_df = pd.DataFrame()

    def set_data(
        self,
        stablecoins_df: pd.DataFrame,
        chains_df: pd.DataFrame,
        # prices_df: pd.DataFrame,
        # yields_df: pd.DataFrame,
    ) -> None:
        """Set the data to be processed.

        Args:
            stablecoins_df: DataFrame containing stablecoin data
            chains_df: DataFrame containing chain distribution data
            prices_df: DataFrame containing price data
            yields_df: DataFrame containing yield data
        """
        self.stablecoins_df = stablecoins_df
        self.chains_df = chains_df
        # self.prices_df = prices_df
        # self.yields_df = yields_df

    def process_stablecoins_data(self) -> pd.DataFrame:
        """Process the stablecoins data.

        Returns:
            DataFrame containing processed stablecoin data
        """
        df = self.stablecoins_df.copy()
        df["circulation_change_day"] = df["circulating"] - df["circulatingPrevDay"]
        df["circulation_change_week"] = df["circulating"] - df["circulatingPrevWeek"]
        df["circulation_change_month"] = df["circulating"] - df["circulatingPrevMonth"]

        df["circulation_change_percentage_day"] = (
            df["circulation_change_day"] / df["circulatingPrevDay"]
        )
        df["circulation_change_percentage_week"] = (
            df["circulation_change_week"] / df["circulatingPrevWeek"]
        )
        df["circulation_change_percentage_month"] = (
            df["circulation_change_month"] / df["circulatingPrevMonth"]
        )
        return df.sort_values(by=["circulation_change_percentage_day", "circulating"])
