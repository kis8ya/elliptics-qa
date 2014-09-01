import time
import json

from collections import namedtuple

from test_helper.logging_tests import logger


class OvertimeError(Exception):
    """Raised when Elliptics transaction took more time than upper bound for an
    expected time.
    """
    pass


def _follow(logfile):
    """Follows a log-file in real-time like `tail -f` in Unix."""
    logfile.seek(0, 2)
    while True:
        line = logfile.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line


def _filter_destructions(loglines):
    """Filters destruction records."""
    for line in loglines:
        info_start = line.find("destruction READ trans: ")
        if info_start != -1:
            # Parsing logged destruction record for READ operation
            destruction_record = [tuple(field_value.strip().split(": "))
                                  for field_value in line[info_start:].split(", ")]
            destruction_record = dict(destruction_record)
            yield destruction_record


def get_logged_destructions(session, log_file):
    """Returns iterable with logged destruction records."""
    loglines = _follow(open(log_file))
    # Write some data to create some new records in log file
    session.write_data("key", "?")
    # Start following log file
    next(loglines)
    return _filter_destructions(loglines)


class RequestsCounter(dict):
    """Amount of requests that went to hosts (with Elliptics node)."""

    TransCheckerParams = namedtuple('TransCheckerParams', ["logged_destructions",
                                                           "case",
                                                           "checked_delay",
                                                           "checked_delay_expected_time"])

    def __init__(self, checker_params):
        self._checker_params = checker_params
        super(RequestsCounter, self).__init__()

    def _check_trans_time(self):
        """Checks READ transaction time."""
        params = self._checker_params
        destruction_record = next(params.logged_destructions)

        host = destruction_record["st"].split(":")[0]
        if params.case[host]["delay"] == params.checked_delay and \
           int(destruction_record["time"]) > params.checked_delay_expected_time:
            raise OvertimeError("READ transaction is overtimed")

    def __getitem__(self, key):
        # if the key is not in items, then set it's value to 0
        try:
            super(RequestsCounter, self).__getitem__(key)
        except KeyError:
            self.__setitem__(key, 0, check=False)

        return super(RequestsCounter, self).__getitem__(key)

    def __setitem__(self, key, value, check=True):
        if check:
            self._check_trans_time()
        super(RequestsCounter, self).__setitem__(key, value)


def do_requests(session, key, requests_number, trans_checker_params):
    """Does specified amount of READ requests and returns distribution of these
    requests.
    """
    requests_count = RequestsCounter(trans_checker_params)

    for _ in xrange(requests_number):
        result = session.read_data(key).get().pop()
        requests_count[result.address.host] += 1

    return requests_count


def stabilizing_requests(session, key, requests_count, retry_max, trans_checker_params):
    """Does specified amount of requests to stabilize weights."""
    retry_number = 0
    while retry_number < retry_max:
        try:
            do_requests(session, key, requests_count, trans_checker_params)
        except OvertimeError as exc:
            logger.info("Failed to stabilize weights - retrying: {}\n".format(exc.message))
            retry_number += 1
        else:
            return
    raise RuntimeError("Retries count ({}) exceeded while was trying to stabilize weights."
                       .format(retry_number))
