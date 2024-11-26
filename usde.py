from src.utils.llama import StableLlamaAPI
import polars as pl


def get_usde_circulating_supply():
    stable_llama = StableLlamaAPI()
    df_usde = stable_llama.get_a_stablecoin(id=146)
    df_eth = pl.DataFrame(df_usde["chainBalances"]["Ethereum"]["tokens"])

    df_eth = df_eth.with_columns(
        (pl.col("date") * 1000).cast(pl.Datetime("ms")).alias("date"),
        pl.col("circulating")
        .struct.field("peggedUSD")
        .cast(pl.Int64)
        .alias("circulating"),
        pl.col("minted").struct.field("peggedUSD").cast(pl.Int64).alias("minted"),
    )

    dfs = {}
    for key in list(df_usde["chainBalances"].keys())[:1]:
        df = pl.DataFrame(df_usde["chainBalances"][key]["tokens"])
        print(df.schema)
        print(df.head())
        df = df.with_columns(
            (pl.col("date") * 1000).cast(pl.Datetime("ms")).alias("date"),
            pl.col("circulating")
            .struct.field("peggedUSD")
            .cast(pl.Int64)
            .alias("circulating"),
            pl.col("minted").struct.field("peggedUSD").cast(pl.Int64).alias("minted"),
        )
        dfs[key] = df
    return dfs


if __name__ == "__main__":
    get_usde_circulating_supply()
