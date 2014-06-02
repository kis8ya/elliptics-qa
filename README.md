elliptics-qa
============

Elliptics quality tests

# Quick Usage

With packages from repository you can run tests tagged as _read-commands_, _indexes_:

    $ PYTHONPATH=lib ./runtests.py \
    --branch=master \
    --testsuite-params='{"_global": {"elliptics_version": "2.25.4.14"}}' \
    --tag=read-commands --tag=indexes
