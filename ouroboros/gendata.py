import pandas as pd

from utils import safe_return

GEN_DATA_COL_NAME = 'DATA'

class GenData:
    def __init__(self):
        self._data = {}

    def clear(self):
        self._data.clear()

    def update(self, time, name, data):
        table = self._data.get(name)
        if table is None:
            table = pd.DataFrame(columns = [GEN_DATA_COL_NAME])
        new_table = table.append(pd.Series([data], name = time, index = [GEN_DATA_COL_NAME]))
        self._data[name] = new_table

    def set_table(self, name, data):
        self._data[name] = data

    @safe_return
    def get(self, time, name):
        table = self._data.get(name)
        if table is not None:
            return table.asof(time)
        return None

    @safe_return
    def get_all(self, name) -> pd.Series:
        return self._data.get(name)[GEN_DATA_COL_NAME]

    @property
    def data(self) -> dict:
        return self._data
