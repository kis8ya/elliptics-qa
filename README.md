# elliptics-qa
Elliptics system tests

## Running tests
There is a script for running tests: [runner/runtests.py](https://github.com/kis8ya/tests-runner/blob/c69aef8ca7ce6d533502ed1fac1069c5a8419ac0/runtests.py). It prepares test bench before all test suites and modifies test bench before each test suite. After preparing test bench it just run py.test tests.

To run tests you need to perform following steps:

### 1 Get test sources

    git clone --recursive -b v2.26 https://github.com/kis8ya/elliptics-qa.git

### 2 Dependencies
Python packages listed in [requirements.txt](requirements.txt). You can install them with following command:

    sudo pip install -r requirements.txt

You need `elliptics-client` installed on your system as well:

    sudo apt-get install elliptics-client

For collecting test artifacts you need to install `zip`:

    sudo apt-get install zip

### 3 Prepare test run config

There is a useful script to generate configuration file for a test run: [elliptics-qa/create_params_file.py](create_params_file.py), but you can create a configuration file with any text editor. Configuration file has following fields:

    {
      "_global": {
        "elliptics_version": <elliptics packages version>,
        "packages_dir": /path/to/elliptics/debs
      },
      <test suite name>: {
        <test suite parameters>
      }
    }

**elliptics_version** and **packages_dir** are mutually exclusive fields: you must provide only one of them.

#### 3a Provide elliptics packages through debian repository

    elliptics-qa$ ./create_params_file.py --path test_parameters.json --elliptics-version=2.26.3.16
    elliptics-qa$ cat test_parameters.json
    {"_global": {"elliptics_version": "2.26.3.16"}}

#### 3b Provide elliptics packages as .deb files

    elliptics-qa$ ./create_params_file.py --path test_parameters.json --packages-dir=/tmp/packages
    elliptics-qa$ cat test_parameters.json
    {"_global": {"packages_dir": "/tmp/packages"}}

### 4 Provide OpenStack environment variables
By default script for running tests use cloud resources to prepare test bench. You need to provide OpenStack environment variables for this script:

    export OS_USERNAME="<domain login>"
    export OS_PASSWORD='<domain password>'
    export OS_AUTH_URL="<identity endpoint>"
    export OS_TENANT_NAME="<project name>"
    export OS_REGION_NAME="<region name>"
    export OS_HOSTNAME_PREFIX="<DNS zone (starts with dot)>"

### 5 Run tests
To run tests tagged as `timeouts` you need to do following commands:

    elliptics-qa$ cd runner
    elliptics-qa/runner$ PYTHONPATH=lib:../lib ./runtests.py \
    >     --configs-dir=../configs \
    >     --instance-name="my-elli-test" \
    >     --testsuite-params=../test_parameters.json \
    >     --user=cloud-user \
    >     --tag=timeouts

`cloud-user` is a default user on cloud instances here.

For more information about runtests.py arguments see `PYTHONPATH=./lib:../lib ./runtests.py --help`.
