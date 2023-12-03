import polars as pl

constraints = pl.DataFrame({"colour": ["red", "green", "blue"], "total": [12, 13, 14]})

part_1 = (
    pl.read_csv(
        # "sample_input.txt",
        "input.txt",
        has_header=False,
        new_columns=["game", "cubes"],
        separator=":",
    )
    .select(
        pl.col("game").str.replace("Game ", "").cast(pl.Int64),
        pl.col("cubes")
        .str.extract_all(r"\d+ [A-z]+")
        .list.eval(
            pl.element().str.split(" ").list.to_struct(fields=["count", "colour"])
        ),
    )
    .explode("cubes")
    .unnest("cubes")
    .group_by("game", "colour")
    .agg(pl.col("count").cast(pl.Int64).max())
    .join(constraints, on="colour", how="left")
    .select(
        pl.col("game")
        .filter((pl.col("count") <= pl.col("total")).all().over("game"))
        .unique()
        .sum()
    )
    .item()
)

print("Part 1:", part_1)


part_2 = (
    pl.read_csv(
        # "sample_input.txt",
        "input.txt",
        has_header=False,
        new_columns=["game", "cubes"],
        separator=":",
    )
    .select(
        pl.col("game").str.replace("Game ", "").cast(pl.Int64),
        pl.col("cubes")
        .str.extract_all(r"\d+ [A-z]+")
        .list.eval(
            pl.element().str.split(" ").list.to_struct(fields=["count", "colour"])
        ),
    )
    .explode("cubes")
    .unnest("cubes")
    .group_by("game", "colour")
    .agg(pl.col("count").cast(pl.Int64).max())
    .group_by("game")
    .agg(pl.col("count").product())
    .select(pl.col("count").sum())
    .item()
)

print("Part 2:", part_2)
