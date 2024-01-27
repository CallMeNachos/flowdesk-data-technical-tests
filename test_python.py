"""Module that contains a Pandas to BigQuery schema translator."""

import datetime
import pandas as pd

data = [
    {
        "column1": 10,
        "column2": 3.14,
        "column3": {"nested1": "A", "nested2": {"nested3": True}},
        "column4": ["X", "Y"],
        "column5": datetime.datetime(2023, 1, 1),
        },
    {
        "column1": 20,
        "column2": 2.71,
        "column3": {"nested1": "B", "nested2": {"nested3": False}},
        "column4": ["Z"],
        "column5": datetime.datetime(2023, 2, 1),
        },
    {
        "column1": 30,
        "column2": 1.23,
        "column3": {"nested1": "C", "nested2": {"nested3": True}},
        "column4": ["W", "V"],
        "column5": datetime.datetime(2023, 3, 1),
        },
    ]
df = pd.DataFrame(data)
print(df)


def generate_bigquery_schema_from_pandas(df: pd.DataFrame) -> list[dict]:
    """
    Generate a BigQuery schema from a pandas dataframe
    :param df: A pandas dataframe
    :return: bq_schema: A BigQuery schema output
    """

    # Map pandas and BigQuery types
    type_mapping = {
        "int64": "INTEGER",
        "float64": "FLOAT",
        "object": "STRING",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN"
        }

    bq_schema = []

    # Check the column type
    for column, pd_type in df.dtypes.items():
        # Get value from df column
        value = df[column].iloc[0]
        mode = "REPEATED" if isinstance(value, list) else "NULLABLE"
        bq_type = "RECORD" if isinstance(value, dict) else type_mapping[pd_type.name]

        # Add a new key called field when the column is a dict
        if isinstance(value, dict):
            schema = {
                "name": column,
                "type": bq_type,
                "mode": mode,
                "fields": ""
                }

            # Handle a nested dict
            nested = []
            for nested_key, nested_value in value.items():
                if isinstance(nested_value, dict):
                    mode = "REPEATED" if isinstance(nested_value, list) else "NULLABLE"
                    bq_type = "RECORD" if isinstance(nested_value, dict) else type_mapping[pd_type.name]
                    nested_fields = {
                        "name": nested_key,
                        "type": bq_type,
                        "mode": mode,
                        "fields": ""
                        }

                    # Handle boolean values
                    bool_fields = []
                    for key, value in nested_value.items():
                        if isinstance(value, bool):
                            bool_nested_fields_ = {
                                "name": key,
                                "type": "BOOLEAN",
                                "mode": "NULLABLE"
                                }
                        bool_fields.append(bool_nested_fields_)
                    nested_fields.update(fields=bool_fields)
                else:
                    # when it's not a dict
                    df_json = pd.json_normalize(value)[nested_key]
                    val = df_json.iloc[0]
                    mode = "REPEATED" if isinstance(val, list) else "NULLABLE"
                    nested_fields = {
                        "name": nested_key,
                        "type": type_mapping[df_json.dtypes.name],
                        "mode": mode
                        }

                nested.append(nested_fields)
            schema.update(fields=nested)

        else:
            schema = {
                "name": column,
                "type": bq_type,
                "mode": mode
                }
        bq_schema.append(schema)

    return bq_schema


if __name__ == "__main__":
    bq_schema_output = generate_bigquery_schema_from_pandas(df)
    print(f"Below the output:\n{bq_schema_output}")
