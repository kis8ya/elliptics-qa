elliptics-qa
============

Elliptics quality tests

Quick Usage
-----------

With packages from repository you can run tests tagged as _read-commands_, _indexes_ with the following command: 

    $ PYTHONPATH=lib ./runtests.py \
    --testsuite-params=./params.json \
    --tag=read-commands --tag=indexes

where **params.json** contains:

    {
      "_global": {
        "elliptics_version": "2.25.4.14",
        "elliptics_config": "templates/elliptics.json.j2"
      }
    }
