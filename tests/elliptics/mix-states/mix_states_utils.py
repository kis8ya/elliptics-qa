import time
import json

from collections import namedtuple, defaultdict

from test_helper.logging_tests import logger


def _readline(logfile):
    """Reads and returns one line from the file.

    The line terminator is '\n'.

    """
    # Read until the line is present
    line = logfile.readline()
    while not line:
        time.sleep(0.1)
        line = logfile.readline()
    # Read until the newline is found
    while line[-1] != "\n":
        line += logfile.readline()

    return line


def _follow(logfile):
    """Follows a log-file in real-time like `tail -f` in Unix."""
    logfile.seek(0, 2)
    while True:
        line = _readline(logfile)
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


TransCheckerParams = namedtuple('TransCheckerParams', ["logged_destructions",
                                                       "case",
                                                       "checked_delay",
                                                       "checked_delay_expected_time"])


class RequestsResult(namedtuple('RequestsResult', ["result", "statistics", "failed_addr"])):
    def __new__(cls, result=False, statistics=defaultdict(int), failed_addr=None):
        return super(RequestsResult, cls).__new__(cls, result, statistics, failed_addr)


def _check_trans_time(params):
    """Checks READ transaction time and returns host of failed transaction."""
    destruction_record = next(params.logged_destructions)
    host = destruction_record["st"].split(":")[0]
    if params.case[host]["delay"] == params.checked_delay and \
       int(destruction_record["time"]) > params.checked_delay_expected_time:
        logger.info("Transaction overtimed: {}\n".format(json.dumps(destruction_record, indent=4)))
        return host
    else:
        return None


def do_requests(session, key, requests_number, trans_checker_params):
    """Makes specified amount of READ requests and returns distribution of these
    requests.
    """
    requests_statistics = defaultdict(int)

    for _ in xrange(requests_number):
        result = session.read_data(key).get().pop()

        check_result = _check_trans_time(trans_checker_params)
        if check_result:
            return RequestsResult(False, failed_addr=check_result)

        requests_statistics[result.address.host] += 1

    return RequestsResult(True, requests_statistics)


def do_requests_with_retry(session, key, requests_count, retry_max, trans_checker_params):
    """Makes specified amount of READ requests with retries."""
    retry_number = 1
    requests_info = RequestsResult()
    while retry_number <= retry_max and not requests_info.result:
        logger.info("Trying to do {} requests ({}/{} try).\n".format(requests_count,
                                                                     retry_number,
                                                                     retry_max))
        requests_info = do_requests(session, key, requests_count, trans_checker_params)
        retry_number += 1
    return requests_info
