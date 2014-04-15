def pytest_addoption(parser):
    parser.addoption('--files_number', type='int', default='127')
    parser.addoption('--node', type='string', action='append')
