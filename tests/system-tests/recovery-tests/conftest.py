def pytest_addoption(parser):
    parser.addoption('--files_number', type='int', default='127',
                     help="Amount of files to write.")
    parser.addoption('--files_size', type='int', default=0,
                     help="Amount of bytes for a single file.")
    parser.addoption('--node', type='string', action='append', dest="nodes",
                     help="Elliptics node. Example: --node=hostname:port:group")
