import polars as pl

# Part 1

part_1 = (
    pl.read_csv("input.txt", has_header=False, new_columns=["data"])
    .select(
        pl.col("data")
        .str.extract_all(r"\d")
        .list.gather([0, -1])
        .list.join("")
        .cast(pl.UInt64)
        .sum()
    )
    .item()
)

print("Part 1:", part_1)


# Part 2

nums = {
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
}
nums_reversed = {k[::-1]: v for k, v in nums.items()}

pat = "|".join(nums) + r"|\d"
pat_reversed = "|".join(nums_reversed) + r"|\d"


part_2 = (
    pl.read_csv("input.txt", has_header=False, new_columns=["data"])
    .select(
        pl.concat_str(
            pl.col("data").str.extract(pat, 0).replace(nums),
            pl.col("data")
            .str.reverse()
            .str.extract(pat_reversed, 0)
            .replace(nums_reversed),
        )
        .cast(pl.UInt64)
        .sum()
    )
    .item()
)

print("Part 2:", part_2)
