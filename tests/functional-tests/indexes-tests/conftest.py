def pytest_addoption(parser):
    parser.addoption('--node', action='append', dest="nodes")
    parser.addoption('--batches-number', type='int', dest="batches_number", default=100)
    parser.addoption('--files-per-batch', type='int', dest="files_per_batch", default=1000)
