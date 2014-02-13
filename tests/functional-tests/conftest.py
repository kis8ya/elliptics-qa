# -*- coding: utf-8 -*-
#
#TODO: заменить Exception на соответствующие исключения elliptics'а во всех тестах
def pytest_addoption(parser):
    parser.addoption('--wait_timeout', type='int')
    parser.addoption('--check_timeout', type='int')
    parser.addoption('--host', type='string', action='append')
