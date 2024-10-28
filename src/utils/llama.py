from typing import Optional, Dict, Any
import requests
import polars as pl
from typing import List


class DefiLlamaAPIError(Exception):
    """Custom exception for DefiLlama API errors"""

    pass


class DefiLlamaAPI:
    """A Python wrapper for the DefiLlama API"""

    BASE_DOMAIN = "llama.fi"
    CIRCULATION_COLS = [
        "circulating",
        "circulatingPrevDay",
        "circulatingPrevWeek",
        "circulatingPrevMonth",
    ]

    def __init__(
        self,
        name: str = "stablecoins",
        endpoint: str = "stablecoins",
        timeout: int = 30,
    ):
        """
        Initialize DefiLlama API client

        Args:
            name (str): Type of data to fetch (e.g., "stablecoins")
            timeout (int): Request timeout in seconds
        """
        self.name = name
        self.endpoint = endpoint
        self.timeout = timeout
        self.base_url = (
            f"https://{self.name}.{self.BASE_DOMAIN}/{self.endpoint}?includePrices=true"
        )

    def _make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict:
        """
        Make HTTP request to DefiLlama API

        Args:
            endpoint (str): API endpoint
            params (dict, optional): Query parameters

        Returns:
            dict: JSON response

        Raises:
            DefiLlamaAPIError: If the API request fails
        """
        try:
            response = requests.get(
                f"{self.base_url}/{endpoint}", params=params, timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise DefiLlamaAPIError(f"API request failed: {str(e)}") from e

    def _process_circulation_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Process circulation columns in the DataFrame

        Args:
            df (pl.DataFrame): Input DataFrame

        Returns:
            pl.DataFrame: Processed DataFrame
        """
        for col in self.CIRCULATION_COLS:
            df = df.with_columns(pl.col(col).struct.field("peggedUSD").alias(f"{col}"))
        return df

    def get_total_supply(self, include_prices: bool = True) -> pl.DataFrame:
        """
        Fetch and process total supply data

        Args:
            include_prices (bool): Whether to include price data in the response

        Returns:
            pl.DataFrame: Processed supply data

        Raises:
            DefiLlamaAPIError: If the API request fails
        """
        params = {"includePrices": str(include_prices).lower()}

        data = self._make_request(self.name, params=params)
        df = pl.DataFrame(data["peggedAssets"])
        df = self._process_circulation_data(df)
        # Add 24h change calculations
        df = df.with_columns(
            [
                (pl.col("circulating") - pl.col("circulatingPrevDay")).alias(
                    "change_24h"
                ),
                (
                    (
                        (pl.col("circulating") - pl.col("circulatingPrevDay"))
                        / pl.col("circulatingPrevDay")
                    )
                    * 100
                )
                .round(2)
                .alias("change_24h_pct"),
            ]
        )

        return df.drop(
            [
                "chainCirculating",
                "chains",
                "price",
                "circulatingPrevWeek",
                "circulatingPrevMonth",
            ]
        ).sort(by="circulating", descending=True, nulls_last=True)

    def _process_coin_chain_data(
        self, coin_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Process chain-specific data for a single coin

        Args:
            coin_data (dict): Raw coin data from API

        Returns:
            list: List of processed chain-specific data dictionaries
        """
        chain_data = []
        for chain, values in coin_data.get("chainCirculating", {}).items():
            try:
                current = list(values["current"].values())[0]
                prev_day = list(values["circulatingPrevDay"].values())[0]

                chain_data.append(
                    {
                        "name": coin_data["name"],
                        "symbol": coin_data["symbol"],
                        "chain": chain,
                        "current": current,
                        "prev_day": prev_day,
                        "price": coin_data.get("price"),
                    }
                )
            except Exception as e:
                self._handle_error(
                    f"Error processing {coin_data['name']} on {chain}: {str(e)}"
                )
                continue

        return chain_data

    def get_chain_circulation(self, include_prices: bool = True) -> pl.DataFrame:
        """
        Get circulation data broken down by chain for all stablecoins

        Args:
            include_prices (bool): Whether to include price data

        Returns:
            pl.DataFrame: Chain-specific circulation data with calculated changes

        Raises:
            DefiLlamaAPIError: If the API request fails
        """
        params = {"includePrices": str(include_prices).lower()}
        data = self._make_request(self.name, params=params)

        all_chain_data = []
        for coin_data in data["peggedAssets"]:
            all_chain_data.extend(self._process_coin_chain_data(coin_data))

        if not all_chain_data:
            raise DefiLlamaAPIError("No valid chain data found")

        df = pl.DataFrame(all_chain_data)
        return df.with_columns(
            [
                (pl.col("current") - pl.col("prev_day")).alias("change_24h"),
                (((pl.col("current") - pl.col("prev_day")) / pl.col("prev_day")) * 100)
                .round(2)
                .alias("change_24h_pct"),
            ]
        )

    def _handle_error(self, message: str) -> None:
        """
        Handle errors consistently throughout the class

        Args:
            message (str): Error message to log/print
        """
        # In a production environment, you might want to use proper logging here
        print(message)
