def pytest_addoption(parser):
    parser.addoption('--backends-number', type=int, dest="backends_number")
