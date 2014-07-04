"""Indexes tests.

These tests check that commands `list_indexes`, `find_all_indexes` and `find_any_indexes`
will return correct results after each of the following changes in indexes:

1. Basic setting indexes (`set_indexes` command).
2. Changing indexes (`set_indexes` command).
3. Updating indexes (`update_indexes` command).
4. Removing indexes (`remove_indexes` command).
5. Internal updating indexes (`update_indexes_internal` command).

"""

import pytest
import random
import itertools
import elliptics

import test_helper.elliptics_testhelper as et

from test_helper.elliptics_testhelper import nodes
from hamcrest import assert_that, not_none, equal_to, has_length, greater_than


indexes_combinations_classes = {
    "SINGLE": lambda seq: random.sample(seq, 1),
    "PART": lambda seq: random.sample(seq, random.randint(2, len(seq) - 1)),
    "FULL": lambda seq: seq
}


def sample_classes(seq, classes):
    """Samples test cases with given equivalence classes and returns them."""
    result = {}
    for name, class_func in classes.items():
        result[name] = class_func(seq)
    return result


def hex_to_id(hex_string):
    """Returns elliptics.Id from hex string."""
    result = [hex_string[i:i + 2] for i in xrange(0, len(hex_string), 2)]
    result = [int(x, 16) for x in result]
    result = elliptics.Id(result)
    return result


@pytest.fixture(scope='module')
def indexes():
    """Generates indexes for the tests."""
    indexes_count = 5
    indexes = [str(j) for j in random.sample(xrange(100000000), indexes_count)]
    return indexes


@pytest.fixture(scope='function', params=indexes_combinations_classes.keys())
def index_list(request, indexes):
    """Prepares test cases for testing `find_all_indexes` and `find_any_indexes` commands."""
    sample_cases = indexes_combinations_classes[request.param]
    return sample_cases(indexes)


@pytest.fixture(scope='module')
def client(nodes):
    """Prepares elliptics session with long timeouts.

    It also patches `elliptics.Session.transform` to return `str(elliptics.Id)`.

    """
    client = et.EllipticsTestHelper(nodes=nodes, wait_timeout=45, check_timeout=60)

    elliptics_transform = client.transform
    client.transform = lambda x: str(elliptics_transform(x))

    return client


class IterableData(object):
    """Iterable by n elements sequence."""
    def __init__(self, max_count):
        self._irange = (x for x in xrange(max_count))

    def nextn(self, count):
        """Returns next n elements."""
        islice = itertools.islice(self._irange, 0, count)
        result = [str(i) for i in islice]
        return result


@pytest.fixture(scope='module')
def ids(pytestconfig, client, indexes):
    """Prepares data for the tests after basic setting indexes (1).

    The data is a dictionary with the following format:

        {
          <key_1>: {
            <index_1>: <key-index_1_data>,
            ...
          },
          ...
        }

    This fixture writes keys (their number depends on test parameters) to elliptics.
    Then adds these keys to randomly chosen indexes.

    """
    batches_count = pytestconfig.option.batches_number
    files_count = pytestconfig.option.files_per_batch

    count = batches_count * files_count
    data = '?'
    ids = {}
    idata = IterableData(count * len(indexes))

    for batch_number in xrange(batches_count):
        async_results = []
        for file_number in xrange(files_count):
            number = batch_number * files_count + file_number
            key = str(number)
            key_id = client.transform(key)
            # Write key
            async_results.append(client.write_data(key, data))

            key_indexes_count = random.randint(1, len(indexes) - 1)
            key_indexes = random.sample(indexes, key_indexes_count)
            indexes_ids = [client.transform(i) for i in key_indexes]
            key_index_data_list = idata.nextn(len(indexes_ids))
            # Set indexes for the current key
            async_results.append(client.set_indexes(key, key_indexes, key_index_data_list))

            ids[key_id] = dict(zip(indexes_ids, key_index_data_list))

        # Wait for asynchronous commands (write_data and set_indexes) to be finished
        for async_result in async_results:
            async_result.wait()

    return ids


@pytest.fixture(scope='module')
def changed_ids(pytestconfig, client, ids, indexes):
    """Prepares data for the tests after changing indexes (2).

    Changes indexes for randomly chosen keys and returns dictionary only with changed data.

    """
    count = pytestconfig.option.files_per_batch

    changed_ids = {}
    ids_to_change = random.sample(ids.keys(), count)

    idata = IterableData(count * len(indexes))
    async_results = []
    for id_to_change in ids_to_change:
        key_id = hex_to_id(id_to_change)
        # Choose new random indexes for the key
        key_indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, key_indexes_count)
        indexes_ids = [client.transform(i) for i in key_indexes]
        key_index_data_list = idata.nextn(len(indexes_ids))
        # Change indexes for the key
        async_results.append(client.set_indexes(key_id, key_indexes, key_index_data_list))

        ids[id_to_change] = dict(zip(indexes_ids, key_index_data_list))

        changed_ids[id_to_change] = ids[id_to_change]

    for async_result in async_results:
        async_result.wait()

    return changed_ids


@pytest.fixture(scope='module')
def updated_ids(pytestconfig, client, ids, indexes):
    """Prepares data for the tests after updating indexes (3).

    Updates indexes for randomly chosen keys and returns dictionary only with updated data.

    """
    count = pytestconfig.option.files_per_batch

    updated_ids = {}
    ids_to_update = random.sample(ids.keys(), count)

    idata = IterableData(count * len(indexes))
    async_results = []
    for id_to_update in ids_to_update:
        key_id = hex_to_id(id_to_update)
        # Choose new random indexes for the key
        key_indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, key_indexes_count)
        indexes_ids = [client.transform(i) for i in key_indexes]
        key_index_data_list = idata.nextn(len(indexes_ids))
        # Update indexes for the key
        async_results.append(client.update_indexes(key_id, key_indexes, key_index_data_list))

        ids[id_to_update].update(dict(zip(indexes_ids, key_index_data_list)))

        updated_ids[id_to_update] = ids[id_to_update]

    for async_result in async_results:
        async_result.wait()

    return updated_ids


@pytest.fixture(scope='module')
def internal_updated_ids(pytestconfig, client, ids, indexes):
    """Prepares data for the tests after internal updating indexes (4).

    Updates indexes (with `update_indexes_internal` command) for randomly chosen keys
    and returns dictionary only with updated data.

    """
    count = pytestconfig.option.files_per_batch

    internal_updated_ids = {}
    ids_to_update = random.sample(ids.keys(), count)

    idata = IterableData(count * len(indexes))
    async_results = []
    for id_to_update in ids_to_update:
        key_id = hex_to_id(id_to_update)
        # Choose new random indexes for the key
        key_indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, key_indexes_count)
        indexes_ids = [client.transform(i) for i in key_indexes]
        key_index_data_list = idata.nextn(len(indexes_ids))
        # Update indexes for the key
        result = client.update_indexes_internal(key_id, key_indexes, key_index_data_list)
        async_results.append(result)

        internal_updated_ids[id_to_update] = ids[id_to_update].copy()

        ids[id_to_update].update(dict(zip(indexes_ids, key_index_data_list)))

    for async_result in async_results:
        async_result.wait()

    return internal_updated_ids


@pytest.fixture(scope='module')
def removed_ids(pytestconfig, client, ids):
    """Prepares data for the tests after removing indexes (4).

    Remove indexes for randomly chosen keys and returns dictionary only with removed data.

    """
    count = pytestconfig.option.files_per_batch

    removed_ids = {}
    ids_to_remove = random.sample(ids.keys(), count)

    async_results = []
    for id_to_remove in ids_to_remove:
        key_id = hex_to_id(id_to_remove)

        key_indexes = []
        indexes_ids = []
        key_index_data_list = []
        # Remove indexes for the key
        async_results.append(client.set_indexes(key_id, key_indexes, key_index_data_list))

        ids[id_to_remove] = dict(zip(indexes_ids, key_index_data_list))

        removed_ids[id_to_remove] = ids[id_to_remove]

    for async_result in async_results:
        async_result.wait()

    return removed_ids


def check_find_all_indexes(index_list, client, ids):
    """Checks `find_all_indexes` command.

    Gets result from `find_all_indexes` and checks that it matches to given dictionary **ids**.

    """
    result_list = client.find_all_indexes(index_list).get()

    index_id_list = [client.transform(i) for i in index_list]

    expected_keys_number = sum(1 for v in ids.values()
                               if len(set(index_id_list).difference(set(v))) == 0)

    assert_that(result_list, has_length(expected_keys_number))

    for result in result_list:
        result_key_id = str(result.id)
        assert_that(ids.get(result_key_id), not_none())
        for index_id in index_id_list:
            assert_that(ids[result_key_id].get(index_id), not_none())

        # check all indexes (and their data) for a specific key (from find_all_indexes result_list)
        assert_that(result.indexes, has_length(len(index_list)))
        for result_index in result.indexes:
            result_index_id = str(result_index.index)

            assert_that(ids[result_key_id].get(result_index_id), not_none())
            assert_that(result_index.data, equal_to(ids[result_key_id][result_index_id]))


def check_find_any_indexes(index_list, client, ids):
    """Checks `find_any_indexes` command.

    Gets result from `find_any_indexes` and checks that it matches to given dictionary **ids**.

    """
    result_list = client.find_any_indexes(index_list).get()

    index_id_list = [client.transform(i) for i in index_list]

    expected_keys_number = sum(1 for v in ids.values()
                               if len(set(index_id_list).intersection(set(v))) != 0)

    assert_that(result_list, has_length(expected_keys_number))

    for result in result_list:
        result_key_id = str(result.id)
        assert_that(ids.get(result_key_id), not_none())

        index_intersection = set(index_id_list).intersection(ids[result_key_id].keys())
        assert_that(index_intersection, has_length(greater_than(0)))

        # check all indexes (and their data) for a specific key (from find_all_indexes result_list)
        assert_that(result.indexes, has_length(len(index_intersection)))
        for result_index in result.indexes:
            result_index_id = str(result_index.index)

            assert_that(ids[result_key_id].get(result_index_id), not_none())
            assert_that(result_index.data, equal_to(ids[result_key_id][result_index_id]))


def check_list_indexes(client, ids):
    """Checks `list_indexes` command.

    Gets result from `list_indexes` for the each key
    and checks that it matches to given dictionary **ids**.

    """
    for key, indexes in ids.items():
        key_id = hex_to_id(key)
        result_indexes = client.list_indexes(key_id).get()

        assert_that(result_indexes, has_length(len(indexes)))
        for result_index in result_indexes:
            result_index_id = str(result_index.index)
            assert_that(indexes.get(result_index_id), not_none())
            assert_that(indexes[result_index_id], equal_to(result_index.data))


def test_list_indexes(client, ids):
    """Checks `list_indexes` command after basic setting indexes (1)."""
    check_list_indexes(client, ids)


def test_find_all_indexes(index_list, client, ids):
    """Checks `find_all_indexes` command after basic setting indexes (1)."""
    check_find_all_indexes(index_list, client, ids)


def test_find_any_indexes(index_list, client, ids):
    """Checks `find_any_indexes` command after basic setting indexes (1)."""
    check_find_any_indexes(index_list, client, ids)


def test_list_indexes_after_change(client, changed_ids):
    """Checks `list_indexes` command after changing indexes (2)."""
    check_list_indexes(client, changed_ids)


def test_find_all_indexes_after_change(index_list, client, ids):
    """Checks `find_all_indexes` command after changing indexes (2)."""
    check_find_all_indexes(index_list, client, ids)


def test_find_any_indexes_after_change(index_list, client, ids):
    """Checks `find_any_indexes` command after changing indexes (2)."""
    check_find_any_indexes(index_list, client, ids)


def test_list_indexes_after_update(client, updated_ids):
    """Checks `list_indexes` command after updating indexes (3)."""
    check_list_indexes(client, updated_ids)


def test_find_all_indexes_after_update(index_list, client, ids):
    """Checks `find_all_indexes` command after updating indexes (3)."""
    check_find_all_indexes(index_list, client, ids)


def test_find_any_indexes_after_update(index_list, client, ids):
    """Checks `find_any_indexes` command after updating indexes (3)."""
    check_find_any_indexes(index_list, client, ids)


def test_list_indexes_after_remove(client, removed_ids):
    """Checks `list_indexes` command after removing indexes (4)."""
    check_list_indexes(client, removed_ids)


def test_find_all_indexes_after_remove(index_list, client, ids):
    """Checks `find_all_indexes` command after removing indexes (4)."""
    check_find_all_indexes(index_list, client, ids)


def test_find_any_indexes_after_remove(index_list, client, ids):
    """Checks `find_any_indexes` command after removing indexes (4)."""
    check_find_any_indexes(index_list, client, ids)


def test_list_indexes_after_internal_update(client, internal_updated_ids):
    """Checks `list_indexes` command after internal updating indexes (5)."""
    check_list_indexes(client, internal_updated_ids)


def test_find_all_indexes_after_internal_update(index_list, client, ids):
    """Checks `find_all_indexes` command after internal updating indexes (5)."""
    check_find_all_indexes(index_list, client, ids)


def test_find_any_indexes_after_internal_update(index_list, client, ids):
    """Checks `find_any_indexes` command after internal updating indexes (5)."""
    check_find_any_indexes(index_list, client, ids)
