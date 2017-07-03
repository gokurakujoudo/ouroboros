import pandas as pd
from dataprovider import DataDefinition, DataProvider

TEST_DATASET_PATH = 'test/test_dataset.h5'

const_table = pd.read_hdf(TEST_DATASET_PATH, 'const')
price_table = pd.read_hdf(TEST_DATASET_PATH, 'price')
id_range = set(const_table.index)
start_date = price_table.index.min()
end_date = price_table.index.max()


def get_test_dataset_definition():
    price = DataDefinition.new_table('PRICE', id_range)
    const = DataDefinition.new_table('CONST', id_range, is_time_series = False)
    definition = DataDefinition.new_definition(price, const)
    return definition


def get_test_dataset():
    data = {'PRICE': price_table, 'CONST': const_table}
    return data


if __name__ == '__main__':
    dp = DataProvider(get_test_dataset_definition(), start_date, end_date, get_test_dataset())
    print(dp.check_dataset_validity())
