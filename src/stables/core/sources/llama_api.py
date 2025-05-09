"""DeFiLlama API client."""

import aiohttp
import asyncio
from typing import Dict, List, Union
from stables.config.settings import DEFILLAMA_STABLECOINS_API_URL, DEFILLAMA_TIMEOUT
from stables.config.logging import setup_logging

logger = setup_logging()


class LlamaStableAPI:
    """Client for interacting with the DeFiLlama Stablecoins API."""

    def __init__(self, path: str = ""):
        """
        Initialize the DeFiLlama API client.

        Args:
            path: Optional path to append to the base URL.
        """
        self.base_url = DEFILLAMA_STABLECOINS_API_URL

        self.timeout = aiohttp.ClientTimeout(total=DEFILLAMA_TIMEOUT)

    async def get_circulating(self) -> List[Dict]:
        """Get chain-specific data for a stablecoin."""
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                async with session.get(f"{self.base_url}/stablecoins") as response:
                    if response.status == 200:
                        response = await response.json()
                        data = response["peggedAssets"]
                        return data
                    else:
                        logger.error(f"Error fetching chain data {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Exception fetching chain data: {str(e)}")
                return []

    async def get_historical(self, id: str) -> List[Dict]:
        """Get historical stablecoin"""
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                async with session.get(f"{self.base_url}/stablecoin/{id}") as response:
                    if response.status == 200:
                        response = await response.json()
                        return response
                    else:
                        logger.error(
                            f"Error fetching historical stablecoin data {response.status}"
                        )
                        return []
            except Exception as e:
                logger.error(f"Exception fetching historical stablecoin data: {str(e)}")
                return []
