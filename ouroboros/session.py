from dataprovider import DataProvider, DataDefinition
from gendata import GenData
from strategy import Strategy
import pandas as pd


class InternalDataProvider:
    def __init__(self):
        self.get_ts = None
        self.get_ts_asof = None
        self.get_const = None
        self.get_table = None
        self.add_gen = None
        self.get_gen = None
        self.get_gen_all = None
        self.get_arg = None
        self.get_cur = None
        self.set_cur = None


class Session:
    def __init__(self, strategy: Strategy, data: DataProvider, start_time, end_time):
        """
        Initialize new instance of back test session
        :type strategy: Strategy
        :type data: DataProvider
        """
        if not DataDefinition.match(strategy.data_def, data.definition):
            raise TypeError("definitions don't match")

        self._strategy = strategy
        self._start_time = pd.to_datetime(start_time)
        self._end_time = pd.to_datetime(end_time)

        self._schedule = None

        self._data = data
        self._gen_data = GenData()
        self._cur_data = {}

        idp = InternalDataProvider()
        idp.get_ts = self._data.get_ts
        idp.get_ts_asof = self._data.get_ts_asof
        idp.get_const = self._data.get_consts
        idp.get_table = self._data.get_table
        idp.add_gen = self._gen_data.update
        idp.get_gen = self._gen_data.get
        idp.get_gen_all = self._gen_data.get_all
        idp.get_arg = self._strategy.get_arg
        idp.get_cur = self._cur_data.get
        idp.set_cur = self._cur_data.__setitem__
        self._idp = idp
        pass

    def _build_schedule(self) -> pd.DataFrame:
        schs = []
        for ef in self._strategy.event_funcs:
            ts = pd.DatetimeIndex(start = self._start_time,
                                  end = self._end_time,
                                  freq = ef.freq)
            if not pd.isnull(ef.lag):
                ts += pd.Timedelta(ef.lag)
            ts = ts[(ts >= self._start_time) & (ts <= self._end_time)]
            sch = pd.DataFrame(index = ts)
            sch['PRIORITY'] = ef.priority
            sch['FUNC'] = ef
            schs.append(sch)
        return pd.concat(schs).sort_values(by = 'PRIORITY').sort_index()

    def run(self, status_callback = None):
        self._cur_data.clear()
        self._gen_data.clear()
        self._schedule = self._build_schedule()

        for index in self._schedule.index:
            try:
                func = self._schedule.loc[index, 'FUNC']
                result = func(index, self._idp)
            except Exception as ex:
                result = repr(ex)
            self._schedule.loc[index, 'RETURN'] = result
            if status_callback is not None:
                status_callback(time = index, result = result)

        if status_callback is not None:
            status_callback(finished = True)
        pass
