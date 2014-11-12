def pytest_addoption(parser):
    parser.addoption('--dropped-groups-path', dest="dropped_groups_path",
                     help="Path to a file with a list of elliptics groups where bad keys "
                     "will be recovered (in JSON format).")
