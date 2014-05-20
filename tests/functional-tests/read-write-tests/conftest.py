# -*- coding: utf-8 -*-
#
#TODO: replace Exception with a proper exception class from elliptics module
def pytest_addoption(parser):
    parser.addoption('--wait_timeout', type='int')
    parser.addoption('--check_timeout', type='int')
    parser.addoption('--node', type='string', action='append')
