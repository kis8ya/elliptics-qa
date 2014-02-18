# -*- coding: utf-8 -*-
#

def pytest_addoption(parser):
    parser.addoption('--wait_timeout', type='int')
    parser.addoption('--check_timeout', type='int')
    parser.addoption('--node', type='string', action='append')
