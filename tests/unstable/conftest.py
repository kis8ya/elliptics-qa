def pytest_addoption(parser):
    parser.addoption('--good-files-number', type='int', default='127', dest="good_files_number",
                     help="Amount of files to write which will be accessible from all groups.")
    parser.addoption('--bad-files-number', type='int', default='256', dest="bad_files_number",
                     help="Amount of files to write which will be accessible from some groups "
                     "and will be recovered.")
    parser.addoption('--broken-files-number', type='int', default='127', dest="broken_files_number",
                     help="Amount of files to write which will be accessible from some groups "
                     "and will not be recovered.")
    parser.addoption('--file-size', type='int', default=0, dest="file_size",
                     help="Amount of bytes for a single file.")
    parser.addoption('--good-keys-path', dest="good_keys_path",
                     help="Path to a file with a list of good keys (in JSON format).")
    parser.addoption('--bad-keys-path', dest="bad_keys_path",
                     help="Path to a file with a list of bad keys (in JSON format).")
    parser.addoption('--broken-keys-path', dest="broken_keys_path",
                     help="Path to a file with a list of broken keys (in JSON format).")
    parser.addoption('--dropped-groups', dest="dropped_groups",
                     help="Path to a file with a list of elliptics groups where bad keys "
                     "will be recovered (in JSON format).")
