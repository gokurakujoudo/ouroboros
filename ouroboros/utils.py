import logging


def para_not_null(paras: set):
    def wrapper(method):
        def checked_filter(*args, **kwargs):
            for name in paras:
                value = kwargs.get(name)
                if value is None:
                    logging.warning('null parameter: {}'.format(name))
                    return None
            return method(*args, **kwargs)
        return checked_filter
    return wrapper


def para_not_empty(paras: set):
    def wrapper(method):
        def checked_filter(*args, **kwargs):
            for name in paras:
                value = kwargs.get(name)
                if value is None or not len(value):
                    logging.warning('empty parameter: {}'.format(name))
                    return None
            return method(*args, **kwargs)
        return checked_filter
    return wrapper


def safe_return(method):
    def try_run(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as ex:
            logging.warning('method {} raises a exception: {}'.format(method.__name__, repr(ex)))
        return None
    return try_run


