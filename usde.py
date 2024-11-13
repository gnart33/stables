from src.utils.llama import StableLlamaAPI
import polars as pl


def get_usde_circulating_supply():
    stable_llama = StableLlamaAPI()
    df_usde = stable_llama.get_a_stablecoin(id=146)
    df_eth = pl.DataFrame(df_usde["chainBalances"]["Ethereum"]["tokens"])
    # print(df_eth.schema)
    # print(df_eth.head())
    df_eth = df_eth.with_columns(
        (pl.col("date") * 1000).cast(pl.Datetime("ms")).alias("date"),
        pl.col("circulating")
        .struct.field("peggedUSD")
        .cast(pl.Int64)
        .alias("circulating"),
        pl.col("minted").struct.field("peggedUSD").cast(pl.Int64).alias("minted"),
    )
    return df_eth


if __name__ == "__main__":
    print(get_usde_circulating_supply())
