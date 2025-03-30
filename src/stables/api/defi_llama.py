"""DeFi Llama API client for fetching stablecoin data."""

from typing import Dict, List, Optional, Any, Union
import aiohttp
import pandas as pd
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class StablesLlamaAPI:
    """API client for fetching stablecoin data from DeFi Llama."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        """Initialize the API client.

        Args:
            session: aiohttp client session for making requests
        """
        self.base_url = "https://stablecoins.llama.fi"
        self.session = session
        self.logger = logging.getLogger(__name__)

    async def _fetch_response(self, endpoint: str) -> Dict:
        """Fetch raw stablecoins data from DeFi Llama API."""
        url = f"{self.base_url}/{endpoint}"
        try:
            async with self.session.get(url) as response:
                response_json = await response.json()
                return response_json
        except Exception as e:
            self.logger.error(f"Error fetching stablecoins data: {e}")
            return None

    async def get_stablecoins(self) -> pd.DataFrame:
        """Fetch stablecoin data from DeFi Llama.

        Returns:
            DataFrame containing stablecoin data with columns:
            - id: Unique identifier
            - name: Stablecoin name
            - symbol: Token symbol
            - gecko_id: CoinGecko ID for the token
            - pegType: Type of peg (e.g. USD, EUR)
            - pegMechanism: Mechanism for maintaining peg
            - priceSource: Source of price data
            - circulating: Current circulating supply
            - circulatingPrevDay: Circulating supply 24h ago
            - circulatingPrevWeek: Circulating supply 7d ago
            - circulatingPrevMonth: Circulating supply 30d ago
            - chains: List of chains where token is available
            - chainData: List of dictionaries with per-chain circulation data
            - price: Current price
        """
        response_json = await self._fetch_response(
            endpoint="stablecoins?includePrices=true"
        )
        if not response_json:
            return pd.DataFrame()

        data = response_json["peggedAssets"]

        stablecoins = []
        for stablecoin in data:
            if isinstance(stablecoin, dict):
                # Get circulating supply values
                circulating_value = self._process_stablecoin_circulating(
                    stablecoin.get("circulating", 0)
                )
                circulating_prev_day = self._process_stablecoin_circulating(
                    stablecoin.get("circulatingPrevDay", 0)
                )
                circulating_prev_week = self._process_stablecoin_circulating(
                    stablecoin.get("circulatingPrevWeek", 0)
                )
                circulating_prev_month = self._process_stablecoin_circulating(
                    stablecoin.get("circulatingPrevMonth", 0)
                )

                # Get chain information
                chain_circulating = stablecoin.get("chainCirculating", {})
                chain_data = self._process_stablecoin_chain_data(chain_circulating)
                chains = [item["chain"] for item in chain_data]

                stablecoins.append(
                    {
                        "id": stablecoin.get("id"),
                        "name": stablecoin.get("name"),
                        "symbol": stablecoin.get("symbol"),
                        "gecko_id": stablecoin.get("gecko_id"),
                        "pegType": stablecoin.get("pegType"),
                        "pegMechanism": stablecoin.get("pegMechanism"),
                        "priceSource": stablecoin.get("priceSource"),
                        "circulating": circulating_value,
                        "circulatingPrevDay": circulating_prev_day,
                        "circulatingPrevWeek": circulating_prev_week,
                        "circulatingPrevMonth": circulating_prev_month,
                        "chains": chains,
                        "chainData": chain_data,
                        "price": stablecoin.get("price", 1.0),
                    }
                )

        return pd.DataFrame(stablecoins)

    async def get_stablecoin_chain_data(self) -> pd.DataFrame:
        """Fetch chain-specific stablecoin data from DeFi Llama.

        Returns:
            DataFrame containing chain-specific data with columns:
            - id: ID of the stablecoin
            - name: Name of the stablecoin
            - symbol: Symbol of the stablecoin
            - chain: Chain name
            - current: Current circulating supply on this chain
            - prevDay: Circulating supply 24h ago
            - prevWeek: Circulating supply 7d ago
            - prevMonth: Circulating supply 30d ago
        """
        response_json = await self._fetch_response(
            endpoint="stablecoins?includePrices=true"
        )
        if not response_json:
            return pd.DataFrame()

        data = response_json["peggedAssets"]

        chain_records = []
        for stablecoin in data:
            if not isinstance(stablecoin, dict):
                continue

            chain_circulating = stablecoin.get("chainCirculating", {})
            for chain, chain_data in chain_circulating.items():
                chain_records.append(
                    {
                        "id": stablecoin.get("id"),
                        "name": stablecoin.get("name"),
                        "symbol": stablecoin.get("symbol"),
                        "chain": chain,
                        "current": self._process_stablecoin_circulating(
                            chain_data.get("current", {})
                        ),
                        "prevDay": self._process_stablecoin_circulating(
                            chain_data.get("circulatingPrevDay", {})
                        ),
                        "prevWeek": self._process_stablecoin_circulating(
                            chain_data.get("circulatingPrevWeek", {})
                        ),
                        "prevMonth": self._process_stablecoin_circulating(
                            chain_data.get("circulatingPrevMonth", {})
                        ),
                    }
                )

        return pd.DataFrame(chain_records)

    def _process_stablecoin_circulating(
        self, circulating_data: Union[Dict, int, float]
    ) -> float:
        """Helper method to calculate total circulating supply from API response."""
        if isinstance(circulating_data, (int, float)):
            return float(circulating_data)
        elif isinstance(circulating_data, dict):
            return sum(
                float(value)
                for value in circulating_data.values()
                if isinstance(value, (int, float))
            )

    def _process_stablecoin_chain_data(self, chain_circulating: Dict) -> List[Dict]:
        """Process chain-specific circulating supply data.

        Args:
            chain_circulating: Dictionary of chain data from API

        Returns:
            List of dictionaries containing per-chain circulation data
        """
        chain_data = []
        for chain, data in chain_circulating.items():
            chain_data.append(
                {
                    "chain": chain,
                    "current": self._process_stablecoin_circulating(
                        data.get("current", {})
                    ),
                    "prevDay": self._process_stablecoin_circulating(
                        data.get("circulatingPrevDay", {})
                    ),
                    "prevWeek": self._process_stablecoin_circulating(
                        data.get("circulatingPrevWeek", {})
                    ),
                    "prevMonth": self._process_stablecoin_circulating(
                        data.get("circulatingPrevMonth", {})
                    ),
                }
            )
        return chain_data

    """
    https://stablecoins.llama.fi/stablecoin/{id}

    """
