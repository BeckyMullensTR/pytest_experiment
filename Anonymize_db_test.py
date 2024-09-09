import pandas as pd
import os
from Anonymize_db import *

def test_shuffle_df():
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})
    shuffled_df = shuffle_df(df)
    assert len(df) == len(shuffled_df)              # same length
    assert set(df['a']) == set(shuffled_df['a'])    # same elements
    assert not df.equals(shuffled_df)               # not the same order

def test_write_dictonary():
    test_dict = {'a': 1, 'b': 2}
    test_name = 'test.json'
    write_dictonary(test_dict, test_name)
    with open(test_name, 'r') as f:                 # file exists and can be opened
        assert json.load(f) == test_dict            # content is as expected
    os.remove(test_name)                            # cleanup