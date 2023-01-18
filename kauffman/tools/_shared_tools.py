import pandas as pd

def as_list(object):
    if type(object) == list:
        return object
    else:
        return [object]