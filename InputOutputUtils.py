from typing import Dict, Tuple
import polars as pl
import polars.selectors as cs
from calendar import monthrange

    
def fixedWidthInputToDataFrame(file_path: str, col_names_and_widths: Dict[str, int], *, skip_rows: int = 0) -> pl.DataFrame:
    
    # Reads a fixed-width file into a dataframe.
    # Strips all values of leading/trailing whitespaces.

    # Args:
    #   col_names_and_widths: A dictionary where the keys are the column names and the values are the widths of the columns.
    

    df = pl.read_csv(
        file_path,
        has_header=False,
        skip_rows=skip_rows,
        new_columns=["full_str"],
        truncate_ragged_lines=True
    )

    # transform col_names_and_widths into a Dict[cols name, Tuple[start, width]]
    slices: Dict[str, Tuple[int, int]] = {}
    start = 0
    for col_name, width in col_names_and_widths.items():
        slices[col_name] = (start, width)
        start += width

    df = df.with_columns(
        [
            pl.col("full_str").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
            for col, slice_tuple in slices.items()
        ]
    ).drop(["full_str"])

    return df

def dlyAsDataFrame( DLY:str) -> pl.DataFrame:

    col_list = {
        "STATION":11, 
        "Year":4,
        "Month":2, 
        "Element":4
    }

    # Use a loop to create the daily columns
    for day in range(31):
        col_list[ str(day)] = 5
        col_list[ str(day) + "_measure"] = 1
        col_list[ str(day) + "_quality_control"] = 1
        col_list[ str(day) + "_source"] = 1

    df = fixedWidthInputToDataFrame( DLY, col_list)
    data_cols = [str(i) for i in range(31)]
    
    '''
    
    '''
    df = df.with_columns(
        daily_values = pl.concat_list( data_cols ),
        qc_flags = pl.concat_list( cs.matches(".*_quality_control.*") )
    ).drop( 
        data_cols 
    ).drop( 
        cs.matches(".*_quality_control.*")
    )
    
    df = df.cast( {"daily_values":pl.List(pl.Float32)}, strict=False )

    
    df = df.with_columns( 
        DATE = pl.date( pl.col("Year"), pl.col("Month"), 1)
    ).cast(
        {"Year":pl.String, "Month":pl.String}
    ).with_columns(
        YEAR_MONTH = pl.col("Year") + pl.col("Month")
    ).drop("Year", "Month")
    
    #Get the total number of days in the month.
    #Hard to believe this isn't a default expression
    #but I promise I checked the API
    df = df.with_columns(
        is_leap_year = pl.col("DATE").dt.is_leap_year()
    )

    daysInMonth = [ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 ]
    #fill in our special case first
    month_whens = pl.when(
        pl.col("DATE").dt.month().is_in([2])  & pl.col("is_leap_year")==True
        ).then( pl.lit(29) )
    
    #then dynamically fill out our default values
    for month in range(len(daysInMonth)):
        month_whens = month_whens.when(
            pl.col("DATE").dt.month() == (month+1)
        ).then(
            pl.lit(daysInMonth[month])
        )
    
    df = df.with_columns(
        days_in_month = month_whens
    ).drop("is_leap_year")
    
    return df