# %%
import polars as pl

# input_file = "sample_input.txt"
input_file = "input.txt"


value_type = (
    pl.when(pl.col("value").str.contains(r"\d"))
    .then(pl.lit("number"))
    .when(pl.col("value") == ".")
    .then(pl.lit("dot"))
    .when(pl.col("value") == "*")
    .then(pl.lit("gear"))
    .otherwise(pl.lit("symbol"))
)

parsed_input = (
    pl.read_csv(input_file, has_header=False, new_columns=["value"])
    .with_row_count("row_index")
    .with_columns(pl.int_ranges(0, pl.col("value").str.len_bytes()).alias("col_index"))
    .explode("value", "col_index")
    # Sort by row index then column index to get sequences of numbers (run lengths)
    .sort("row_index", "col_index")
    .with_columns(
        pl.col("row_index").cast(pl.Int64),
        value_type.alias("value_type"),
        # Id for a sequence of the same type of characters, such as a run of numbers
        value_type.rle_id().alias("rle_id"),
        # Coordinates of all surrounding cells
        pl.concat_list(
            pl.struct(
                adjacent_row_index=pl.col("row_index") + i,
                adjacent_col_index=pl.col("col_index") + j,
            )
            for i in range(-1, 2)
            for j in range(-1, 2)
            # Exclude the cell itself
            if not (i == 0 and j == 0)
        ).alias("adjacent_cells"),
    )
)

# Self join to adjacent cells to get their values
joined = parsed_input.drop("adjacent_cells").join(
    # TODO: Could keep row and col as struct rather than unnest
    parsed_input.explode("adjacent_cells").unnest("adjacent_cells"),
    left_on=["row_index", "col_index"],
    right_on=["adjacent_row_index", "adjacent_col_index"],
)

# %%
part_1 = (
    # We care about number adjacent to symbols
    joined.filter(value_type="number")
    # Does the cell have an adjacent symbol?
    .group_by("row_index", "col_index", "value", "rle_id")
    .agg(
        (pl.col("value_type_right").is_in(["symbol", "gear"]))
        .any()
        .alias("has_adjacent_symbol"),
    )
    # Need to sort so the numbers are concatenated into the list in the correct order
    .sort("row_index", "col_index")
    # Does any cell in the run of numbers have an adjacent symbol?
    # Only one cell needs to have an adjacent symbol for the number to count
    .group_by("rle_id")
    .agg(
        pl.col("value").alias("number"),
        pl.col("has_adjacent_symbol").any(),
    )
    .filter("has_adjacent_symbol")
    .select(pl.col("number").list.join("").cast(pl.Int64).sum())
    .item()
)

print("Part 1:", part_1)

# %%
part_2 = (
    # We care about a `*` being adjacent to exactly 2 runs of numbers
    joined.filter(pl.col("value") == "*", pl.col("value_type_right") == "number")
    .group_by("rle_id")
    .agg(pl.col("rle_id_right").unique().alias("adjacent_rle_id"))
    # Filter for 2 adjacent runs of numbers and explode the adjacent run numbers
    .filter(pl.col("adjacent_rle_id").list.len() == 2)
    .explode("adjacent_rle_id")
    # Join to the adjacent runs to get the values
    .join(parsed_input, left_on="adjacent_rle_id", right_on="rle_id")
    # Need to sort so the numbers are concatenated into the list in the correct order
    .sort("row_index", "col_index")
    .group_by("rle_id", "adjacent_rle_id")
    # Concatenate the runs of numbers into a list of strings
    .agg(pl.col("value").alias("number"))
    .with_columns(pl.col("number").list.join("").cast(pl.Int64))
    # Multiply the two adjacent runs of numbers together
    .group_by("rle_id")
    .agg(pl.col("number").product())
    # And finally, sum all the multiples
    .select(pl.col("number").sum())
    .item()
)

print("Part 2:", part_2)
