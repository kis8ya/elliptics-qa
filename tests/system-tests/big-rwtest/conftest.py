def pytest_addoption(parser):
    parser.addoption('--batch_number', type='int', default='50')
    parser.addoption('--batch_size', type='int', default='50')
    parser.addoption('--check_timeout', type='int', default='60')
    parser.addoption('--wait_timeout', type='int', default='50')
    parser.addoption('--node', type='string', action='append')
