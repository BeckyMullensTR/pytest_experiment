##################################################################################
#
#
##################################################################################
#!pip install pandas

import string
import secrets
import pandas as pd
import json

def write_dictonary(dictonary, name):
    with open(name, "w") as new_file:
        json.dump(dictonary, new_file)

def shuffle_df(dataframe):
    df_shuffled = dataframe.sample(frac=1).reset_index(drop=True)
    return df_shuffled

def publish_df(df, df_name='return_activity.csv'):
    df.to_csv(df_name, index=False)

db_file="database.csv"
db_df = pd.read_csv(db_file)
#db_df = pd.read_csv(db_file)
#db_df

agency_dict = {}
reverse_dict = {}

filename_dict = {}
reverse_filename_dict = {}

agency_placeholders = []
filename_placeholders = []
info = []
ids = []

for index, row in db_df.iterrows():
    if row['agency_id'] is None or row['filename'] is None or row['info'] is None:
        continue
    info.append(str(row['info']))
    ids.append(secrets.token_hex(24))
    agency = str(row['agency_id'])
    filename = str(row['filename'])
    # Verify we already have this agency
    if agency_dict.get(agency) is None:
        # using 24 to preserve 3 bytes.
        agency_placeholder = secrets.token_hex(24)
        agency_dict[agency] = agency_placeholder
        reverse_dict[agency_placeholder] = agency
        agency_placeholders.append(agency_placeholder)
    else:
        agency_placeholer = agency_dict.get(agency)
        agency_placeholders.append(agency_placeholder)
    # verify we already have this filename ??
    if filename_dict.get(filename) is None:
        filename_placeholder = secrets.token_hex(24)
        filename_dict[filename] = filename_placeholder
        reverse_filename_dict[filename_placeholder] = filename
        filename_placeholders.append(filename_placeholder)
    else:
        filename_placeholder = filename_dict.get(filename)
        filename_placeholders.append(filename_placeholder)
                
anonymized_db = {
    'return_id': ids,
    'info': info,
    'agency_placeholder': agency_placeholders,
    'filename_placeholder': filename_placeholders
}

anonymized_df = pd.DataFrame(anonymized_db)
shuffled = shuffle_df(anonymized_df)
publish_df(shuffled, 'return_activity.csv')