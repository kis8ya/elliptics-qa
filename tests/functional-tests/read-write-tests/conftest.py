# -*- coding: utf-8 -*-
#
#TODO: replace Exception with a proper exception class from elliptics module
def pytest_addoption(parser):
    parser.addoption('--node', type='string', action='append', dest="nodes")
