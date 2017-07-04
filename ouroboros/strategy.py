import pandas as pd

class EventFunction:
    def __init__(self, func, priority = 1, freq = '1d', lag = None):
        """
        Initialize a new instance of EventFunction
        :type func: function
        :type priority: int
        :type freq: str
        :type lag: str
        :param func: f(date, sess) -> DateFrame
        :param freq: frequency of the function
        :param priority: priority of the function
        :param lag: lag between time start and first call of function
        """
        self.lag = lag
        self.func = func
        self.freq = freq
        self.priority = priority


class Strategy:
    def __init__(self, data_def: pd.DataFrame, event_funcs: list, arg_dict: dict = None):
        """
        Initialize a new instance of Strategy
        :param data_def: data definition
        :param event_funcs: a list of EventFunction
        :param arg_dict: a dict of parameters
        """
        self.data_def = data_def
        self.event_funcs = event_funcs
        self.arg_dict = arg_dict
        pass
