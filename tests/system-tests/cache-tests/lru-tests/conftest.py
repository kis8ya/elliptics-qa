def pytest_addoption(parser):
    parser.addoption('--requests-number', type='int', dest="requests_number")
    parser.addoption('--allowed-time-diff-rate', type=float, default=0.2,
                     dest="allowed_time_diff_rate")
