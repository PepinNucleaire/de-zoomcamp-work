from re import sub
from pandas import DataFrame

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# Define a function to convert a string to snake case
def snake_case(s):
    # Replace hyphens with spaces, then apply regular expression substitutions for title case conversion
    # and add an underscore between words, finally convert the result to lowercase
    return '_'.join(
        sub('([A-Z][a-z]+)', r' \1',
        sub('([A-Z]+)', r' \1',
        s.replace('-', ' '))).split()).lower()

def transform_df_columns_to_snake_case(df):
    clean_columns = [snake_case(col) for col in list(df.columns)]
    print(f'There are {len(set(df.columns) & set(clean_columns))} columns that were not changed')
    print(f'There are {len(df.columns)-len(set(df.columns) & set(clean_columns))} columns that were changed')

    df.columns = clean_columns
    return df

def filter_rows(df):
    print("Rows with 0 passengers:",df['passenger_count'].isin([0]).sum())
    print("Rows with 0 passengers:",df['trip_distance'].isin([0]).sum())
    df= df[df["passenger_count"]>0]
    df = df[df["trip_distance"]>0]
    return df

def add_pickup_date(df):
    df["lpep_pickup_date"] = df["lpep_pickup_datetime"].dt.date
    return df


@transformer
def transform(data, *args, **kwargs):
    data = transform_df_columns_to_snake_case(data)
    data = filter_rows(data)
    data = add_pickup_date(data)
    print(f'vendor_id unique values are : {list(data["vendor_id"].unique())}')

    return data

    

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test
def test_vendor_id_is_there(output, *args) -> None:
    assert "vendor_id" in list(output.columns), 'vendor_id is not there'

@test
def test_passenger_count_sup_to_0(output, *args) -> None:
    assert len(output) == len(output[output["passenger_count"]>0]), 'there are rows where passenger_count is 0 or less'

@test
def test_trip_distance_sup_to_0(output, *args) -> None:
    assert len(output) == len(output[output["trip_distance"]>0]), 'there are rows where trip_distance is 0 or less'

    

