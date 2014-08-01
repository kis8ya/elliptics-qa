def pytest_addoption(parser):
    parser.addoption('--consistent-files-number', type='int', dest="consistent_files_number",
                     help="Amount of files to write which will be accessible from all groups.")
    parser.addoption('--inconsistent-files-number', type='int', dest="inconsistent_files_number",
                     help="Amount of files to write which will be accessible from some groups.")
    parser.addoption('--file-size', type='int', default=0, dest="file_size",
                     help="Amount of bytes for a single file.")
    parser.addoption('--broken-files-percentage', type=float, dest="broken_files_percentage",
                     help="Percentage of inconsistent keys which will not be recovered.")
    parser.addoption('--dropped-groups-path', dest="dropped_groups_path",
                     help="Path to a file with a list of elliptics groups where bad keys "
                     "will be recovered (in JSON format).")
    parser.addoption('--consistent-keys-path', dest="consistent_keys_path",
                     help="Path to a file with a list of consistent keys (in JSON format).")
    parser.addoption('--inconsistent-keys-path', dest="inconsistent_keys_path",
                     help="Path to a file with a list of inconsistent keys (in JSON format).")
