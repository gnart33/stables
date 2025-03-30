"""Dashboard for stablecoin analytics."""

from typing import Optional
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from ..processor.processor import StablecoinDataProcessor


class Dashboard:
    """Dashboard for displaying stablecoin analytics."""

    def __init__(self, processor: StablecoinDataProcessor) -> None:
        """Initialize the dashboard.

        Args:
            processor: Data processor instance
        """
        self.processor = processor

    def render(self) -> None:
        """Render the dashboard."""
        st.title("📊 Stablecoin Analytics Dashboard")

        # Daily Changes
        st.header("Daily Changes")
        daily_changes = self.processor.get_daily_changes()
        fig = px.bar(
            daily_changes,
            x="symbol",
            y="circulating",
            title="Circulating Supply by Stablecoin",
            labels={"circulating": "Circulating Supply (USD)"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Chain Distribution
        st.header("Chain Distribution")
        chain_dist = self.processor.get_chain_distribution()
        fig = px.pie(
            chain_dist,
            values="percentage",
            names="chain",
            title="Stablecoin Distribution Across Chains",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Top Yields
        st.header("Top Yields")
        top_yields = self.processor.get_top_yields()
        fig = px.bar(
            top_yields,
            x="symbol",
            y="apy",
            title="Top Yielding Stablecoin Pools",
            labels={"apy": "APY (%)"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Price Volatility
        st.header("Price Volatility")
        volatility = self.processor.get_price_volatility()
        fig = px.bar(
            volatility,
            x="symbol",
            y="deviation",
            title="Price Deviation from $1",
            labels={"deviation": "Deviation (%)"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Market Cap Trends
        st.header("Market Cap Trends")
        trends = self.processor.get_market_cap_trends()
        fig = px.line(
            trends,
            x="date",
            y="totalCirculatingUSD",
            title="Total Market Cap Over Time",
            labels={"totalCirculatingUSD": "Total Market Cap (USD)"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Data Tables
        st.header("Raw Data")
        tab1, tab2, tab3, tab4, tab5 = st.tabs(
            [
                "Daily Changes",
                "Chain Distribution",
                "Top Yields",
                "Price Volatility",
                "Market Cap Trends",
            ]
        )

        with tab1:
            st.dataframe(daily_changes)
        with tab2:
            st.dataframe(chain_dist)
        with tab3:
            st.dataframe(top_yields)
        with tab4:
            st.dataframe(volatility)
        with tab5:
            st.dataframe(trends)
