# Assignment

## Brief

Write the Python codes for the following questions.

## Instructions

Paste the answer as Python in the answer code section below each question.

### Question 1

Question: Join the `metadata_pl` and `ratings_pl` DataFrames in Polars, then calculate the average rating by `genre` and by `original_language` (separately).

Answer:

```python
#dataset are too huge, i moved the dataset to same tutotial folder so that i can read from the same folder
import polars as pl
metadata_lf = pl.scan_csv("data/movies_metadata.csv") #lazy read for movie dataset
ratings_lf  = pl.scan_csv("data/ratings.csv") #lazy read for ratings 
joint_lf = ratings_lf.join(metadata_lf, on="movieId", how="inner")

genre_rating = (
    joint_lf
        .with_columns(
            pl.col("genres")
              .str.replace("'", '"')          # Make it valid JSON
              .str.json_extract()            # Extract list of dicts
              .list.eval(pl.element().struct.field("name"))
              .alias("genre")                # Now it's a list of names
        )
        .with_columns(pl.col("genre").explode())
        .groupby("genre")
        .agg(pl.mean("rating").alias("avg_rating_by_genre"))
        .sort("avg_rating_by_genre", descending=True)
        .collect()
)

lang_rating = (
    joint_lf
        .groupby("original_language")
        .agg(pl.mean("rating").alias("avg_rating_by_language"))
        .sort("avg_rating_by_language", descending=True)
        .collect()
)

```

### Question 2

Question: Calculate the average total amount for vendors with at least 5 trips from the NYC Taxi Trip Data.

Answer:

```python

import polars as pl

# Lazy read for big data, i moved the dataset to same tutotial folder
trips_lf = pl.scan_csv("taxi_trip_data.csv")

result = (
    trips_lf
        .groupby("vendor_id")
        .agg([
            pl.count().alias("trip_count"),
            pl.col("total_amount").mean().alias("avg_total_amount")
        ])
        .filter(pl.col("trip_count") >= 5)
        .sort("avg_total_amount", descending=True)
        .collect()
)

result

```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
