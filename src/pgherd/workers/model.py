__author__ = 'mike'

class Node(object):

    is_master = False
    name = ''
    ip = ''
    last_checked_ts = 0
    last_response_ts = 0
    lag_size = 0
    lag_time = 0