from strategy import Strategy, EventFunction
import pandas as pd


class Session:
    def __init__(self, strategy, data, start_time, end_time):
        """
        Initialize new instance of back test session
        :type strategy: Strategy
        :type data: dict
        """
        self._strategy = strategy
        self._data = data
        self._start_time = pd.to_datetime(start_time)
        self._end_time = pd.to_datetime(end_time)
        self._gen_data = {}
        self._schedule = self._build_schedule()
        pass

    def _build_schedule(self) -> pd.DataFrame:
        schs = []
        for ef in self._strategy.event_funcs:
            if isinstance(ef, EventFunction):
                ts = pd.DatetimeIndex(start = self._start_time,
                                      end = self._end_time,
                                      freq = ef.freq)
                if not pd.isnull(ef.lag):
                    ts += pd.Timedelta(ef.lag)
                ts = ts[(ts >= self._start_time) & (ts <= self._end_time)]
                sch = pd.DataFrame(index = ts)
                sch['PRIORITY'] = ef.priority
                sch['FUNC'] = ef.func
                schs.append(sch)
                pass
        return pd.concat(schs).sort_values(by = 'PRIORITY').sort_index()

    # TODO
    def _run(self):
        pass
