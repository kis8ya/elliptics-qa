def pytest_addoption(parser):
    parser.addoption("--backends-number", type=int, help="number of backends.")
