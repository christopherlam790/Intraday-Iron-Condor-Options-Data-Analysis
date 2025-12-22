import sys

sys.path.insert(0, "src/loaders")  # add loaders path to search list
import stock_data_loader




class ValidationError(Exception):
    """Table has a validation error."""
    pass



# Check if the 'timestamp' column exists in the table schema & is of format string
def validate_timestamp(df):
    
    return df





df = stock_data_loader.load_data()

print(validate_timestamp(df=df))