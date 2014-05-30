import apt

def pytest_addoption(parser):
    parser.addoption("--node", action="append", dest="nodes")

def pytest_collection_modifyitems(items):
    apt_cache = apt.Cache()
    package = apt_cache["elliptics-client"]
    version = package.installed.version.split(".")[1]
    marker = "new_version" if version == "25" else "old_version"

    items[:] = [i for i in items if i.get_marker(marker)]
