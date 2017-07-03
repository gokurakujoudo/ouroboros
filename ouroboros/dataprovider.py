import logging
from typing import Optional

import pandas as pd

from utils import para_not_empty, para_not_null, safe_return

CONST_TABLE_NAME = 'CONST'
TIME_COL_NAME = 'index'
ID_COL_NAME = 'variable'
VALUE_COL_NAME = 'value'


class DataDefinition:
    @classmethod
    def new_table(cls, name: str, id_range, is_time_series = True, is_delta = False, is_wide = True):
        return {'NAME'          : name,
                'ID_RANGE'      : id_range,
                'IS_TIME_SERIES': is_time_series,
                'IS_DELTA'      : is_delta,
                'IS_WIDE'       : is_wide}

    @classmethod
    def new_definition(cls, *args):
        if len(args):
            return pd.DataFrame(list(args)).set_index('NAME')


def check_name_wrap(method, name_range, key = 'name', arg_id = 0):
    def checked_name(*arg, **kwargs):
        if kwargs.get(key):
            name = kwargs.get(key)
        elif arg:
            name = arg[arg_id]
        else:
            raise TypeError("Missing argument: '{}'".format(key))
        if name not in name_range:
            raise KeyError("Invalid key: '{}'".format(name))
        return method(*arg, **kwargs)

    return checked_name


class DataProvider:
    def __init__(self, definition: pd.DataFrame, start_time, end_time, dataset = None):
        self._start_time = pd.to_datetime(start_time)
        self._end_time = pd.to_datetime(end_time)
        self._data = {}
        self._definition = definition

        self._time_pos_tables = set(definition[definition.IS_TIME_SERIES & ~definition.IS_DELTA].index)
        self._time_delta_tables = set(definition[definition.IS_TIME_SERIES & definition.IS_DELTA].index)
        self._const_cols = set(definition[~definition.IS_TIME_SERIES].index)
        self._tables = self._time_pos_tables | self._time_delta_tables
        if len(self._const_cols):
            self._tables |= {CONST_TABLE_NAME}

        if dataset is not None:
            self.load(dataset = dataset)

        self.get_table = check_name_wrap(self._get_table, self._tables)
        self.get_ts = check_name_wrap(self._get_ts, self._time_pos_tables | self._time_delta_tables)
        self.get_ts_asof = check_name_wrap(self._get_ts, self._time_pos_tables)

    def clear_data(self):
        self._data = {}
        logging.warning('dataset cleared')

    def load(self, **kwargs) -> bool:
        dataset = kwargs.get('dataset')
        if isinstance(dataset, dict):
            if self.check_dataset_validity(dataset = dataset):
                self._data = dataset
                logging.info('dataset loaded')
                return True

        names = kwargs.get('names')
        tables = kwargs.get('tables')
        if names is None or tables is None:
            logging.warning('failed to load data')
            return False
        if isinstance(names, str) and not isinstance(names, list):
            names = [names]
        if not isinstance(tables, list):
            tables = [tables]
        if len(names) != len(tables):
            logging.warning('failed to load data: names and tables are not match in length')
            return False
        for name, table in zip(names, tables):
            if name in self._tables:
                if isinstance(table, pd.DataFrame):
                    logging.info("load table ['{}']".format(name))
                    self._data[name] = table
                else:
                    logging.warning("['{}'] is not valid table".format(name))
            else:
                logging.warning("['{}'] is not defined in strategy".format(name))
            pass

    def check_dataset_validity(self, dataset: dict = None) -> bool:
        logging.info('check dataset validity')
        if not dataset:
            dataset = self._data
        if isinstance(dataset, dict):
            for name in self._tables:
                table = dataset.get(name)
                if not isinstance(table, pd.DataFrame):
                    logging.warning("check failed: ['{}'] is not pd.Dataframe".format(name))
                    return False
            logging.info('check passed')
            return True
        logging.warning('check failed: dataset is not dist')
        return False

    def _get_table(self, name) -> pd.DataFrame:
        return self._data.get(name)

    @safe_return
    @para_not_empty({'ids'})
    @para_not_null({'start_time', 'end_time'})
    def _get_ts(self, name, **kwargs) -> Optional[pd.DataFrame]:
        ids = kwargs.get('ids')
        start_time = pd.to_datetime(kwargs.get('start_time'))
        end_time = pd.to_datetime(kwargs.get('end_time'))
        table = self._get_table(name)
        return_wide = kwargs.get('return_wide', True)
        if ~self._definition.loc['IS_WIDE', name]:
            # long table
            id_col_name = kwargs.get('id_col', ID_COL_NAME)
            index_col_name = kwargs.get('index_col', TIME_COL_NAME)
            table_slice = table[(table[id_col_name].isin(ids)) &
                                (table[index_col_name] >= start_time) &
                                (table[index_col_name] <= end_time)]
            if return_wide:
                value_col_name = kwargs.get('value_col', VALUE_COL_NAME)
                return table_slice.pivot(index_col_name, id_col_name, value_col_name)
        else:
            # wide table
            table_slice = table.loc[start_time:end_time, ids]
            if ~return_wide:
                id_col_name = kwargs.get('id_col', ID_COL_NAME)
                index_col_name = kwargs.get('index_col', TIME_COL_NAME)
                value_col_name = kwargs.get('value_col', VALUE_COL_NAME)
                table_slice.index.name = index_col_name
                return table_slice.melt(id_vars = index_col_name,
                                        var_name = id_col_name,
                                        value_name = value_col_name).dropna(how = 'any', axis = 0)
        return table_slice

    @safe_return
    @para_not_empty({'ids', 'time_stamps'})
    def _get_ts_asof(self, name, **kwargs) -> Optional[pd.DataFrame]:
        ids = kwargs.get('ids')
        time_stamps = pd.to_datetime(kwargs.get('time_stamps'))
        table = self._get_table(name)
        id_col_name = kwargs.get('id_col_name', ID_COL_NAME)
        index_col_name = kwargs.get('index_col_name', TIME_COL_NAME)
        value_col_name = kwargs.get('value_col_name', VALUE_COL_NAME)
        if ~self._definition.loc['IS_WIDE', name]:
            # long table
            table_wide = table.pivot(index_col_name, id_col_name, value_col_name)
        else:
            # wide table
            table_wide = table
        table_asof = table_wide[ids].asof(time_stamps)
        return_wide = kwargs.get('return_wide', True)
        if return_wide:
            return table_asof
        else:
            table_asof.index.name = index_col_name
            return table_asof.melt(id_vars = index_col_name,
                                   var_name = id_col_name,
                                   value_name = value_col_name).dropna(how = 'any', axis = 0)

    @safe_return
    def get_consts(self, ids, cols) -> Optional[pd.DataFrame]:
        table = self._get_table(CONST_TABLE_NAME)
        return table.loc[ids, cols]
