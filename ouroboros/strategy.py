import pandas as pd


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


    def get_arg(self, key):
        return self.arg_dict.get(key)
