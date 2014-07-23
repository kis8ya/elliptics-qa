import apt

def pytest_addoption(parser):
    parser.addoption("--old-version", dest="old_version")

def pytest_collection_modifyitems(config, items):
    apt_cache = apt.Cache()
    package = apt_cache["elliptics-client"]
    current_version = package.installed.version
    old_version = config.option.old_version
    marker = "old_version" if current_version == old_version else "new_version"

    items[:] = [i for i in items if i.get_marker(marker)]
