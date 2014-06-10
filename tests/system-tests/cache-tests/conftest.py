def pytest_addoption(parser):
    parser.addoption('--node', type='string', action='append', dest="nodes")
    parser.addoption('--show-progress', action='store_true', dest="show_progress")
    parser.addoption('--requests-number', type='int', dest="requests_number")
