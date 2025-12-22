import sys

sys.path.insert(0, "src/loaders")  # add loaders path to search list
import stock_data_loader




class ValidationError(Exception):
    """df has a validation error."""
    pass



def validate_timestamp(df):
    
    if "timestamp_hourly" not in df.columns:
        raise ValidationError("df does not contain a 'timestamp_hourly' column")

    if "timestamp_daily" not in df.columns:
        raise ValidationError("df does not contain a 'timestamp_daily' column")

    return df


df = stock_data_loader.load_data()

print(df.columns.tolist())