def event(priority = 1, freq = '1d', lag = None, condition = None):
    """
    Initialize a new instance of EventFunction
    :type priority: int
    :type freq: str
    :type lag: str
    :param freq: frequency of the function
    :param priority: priority of the function
    :param lag: lag between time start and first call of function
    :param condition: (Optional) precondition of running the function,
    with the same form but return bool
    """

    def event_wrap(method):
        if condition is not None:
            def run(time, idp):
                if condition(time, idp):
                    method(time, idp)
                    return "method finished"
                return "skip main method"

        else:
            def run(time, idp):
                method(time, idp)
                return "method finished"

        run.priority = priority
        run.freq = freq
        run.lag = lag
        return run

    return event_wrap
