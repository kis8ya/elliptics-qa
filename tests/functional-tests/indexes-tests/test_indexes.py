import pytest
import random
import itertools

from collections import defaultdict

import elliptics_testhelper as et

from hamcrest import assert_that, not_none, equal_to, has_length, greater_than

# utility functions
indexes_combinations_classes = {
    "SINGLE": lambda seq: random.sample(seq, 1),
    "PART": lambda seq: random.sample(seq, random.randint(2, len(seq) - 1)),
    "FULL": lambda seq: seq
    }

def sample_classes(seq, classes):
    result = {}
    for name, class_func in classes.items():
        result[name] = class_func(seq)
    return result
#END of utility functions

indexes_count = 5
indexes = map(str, random.sample(xrange(100000000), indexes_count))

@pytest.fixture(scope='module')
def client(pytestconfig):
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    test_helper = et.EllipticsTestHelper(nodes=nodes, wait_timeout=45, check_timeout=60)
    return test_helper

def key_index_data(key_id, index_id):
    """key-index data = <key_id>_<index_id>"""
    data = "{0}_{1}".format(key_id, index_id)
    return data

class IterableData(object):
    def __init__(self, max_count):
        self._irange = (x for x in xrange(max_count))

    def nextn(self, count):
        islice = itertools.islice(self._irange, 0, count)
        result = [i for i in islice]
        result = map(str, result)
        return result

@pytest.fixture(scope='module')
def ids(pytestconfig, client):
    batches_count = pytestconfig.option.batches_number
    files_count = pytestconfig.option.files_per_batch
    count = batches_count * files_count
    data = '?'
    ids = defaultdict(dict)
    idata = IterableData(count * indexes_count)

    for j in xrange(batches_count):
        async_results = []
        for i in xrange(files_count):
            n = j * files_count + i
            key = str(n)
            key_id = str(client.transform(key))
            async_results.append(client.write_data(key, data))

            key_indexes_count = random.randint(1, len(indexes) - 1)
            key_indexes = random.sample(indexes, key_indexes_count)
            indexes_ids = map(client.transform, key_indexes)
            indexes_ids = map(str, indexes_ids)
            key_index_data_list = idata.nextn(len(indexes_ids))

            async_results.append(client.set_indexes(key, key_indexes, key_index_data_list))

            ids[key_id] = dict(zip(indexes_ids, key_index_data_list))

        for r in async_results:
            r.get()

    return ids

@pytest.mark.parametrize(('test_class_name', 'index_list'), sample_classes(indexes, indexes_combinations_classes).items())
def test_find_all_indexes(test_class_name, index_list, client, ids):
    result = client.find_all_indexes(index_list).get()

    index_id_list = map(client.transform, index_list)
    index_id_list = map(str, index_id_list)

    expected_keys_number = sum(1 for v in ids.values()
                               if len(set(index_id_list).difference(set(v))) == 0)

    assert_that(result, has_length(expected_keys_number))

    for r in result:
        result_key_id = str(r.id)
        assert_that(ids.get(result_key_id), not_none())
        for index_id in index_id_list:
            assert_that(ids[result_key_id].get(index_id), not_none())

        # check all indexes (and their data) for a specific key (from find_all_indexes result)
        assert_that(r.indexes, has_length(len(index_list)))
        for ri in r.indexes:
            result_index_id = str(ri.index)

            assert_that(ids[result_key_id].get(result_index_id), not_none())
            assert_that(ri.data, equal_to(ids[result_key_id][result_index_id]))

@pytest.mark.parametrize(('test_class_name', 'index_list'), sample_classes(indexes, indexes_combinations_classes).items())
def test_find_any_indexes(test_class_name, index_list, client, ids):
    result = client.find_any_indexes(index_list).get()

    index_id_list = map(client.transform, index_list)
    index_id_list = map(str, index_id_list)

    expected_keys_number = sum(1 for v in ids.values()
                               if len(set(index_id_list).intersection(set(v))) != 0)

    assert_that(result, has_length(expected_keys_number))

    for r in result:
        result_key_id = str(r.id)
        assert_that(ids.get(result_key_id), not_none())

        index_intersection = set(index_id_list).intersection(ids[result_key_id].keys())
        assert_that(index_intersection, has_length(greater_than(0)))

        # check all indexes (and their data) for a specific key (from find_all_indexes result)
        assert_that(r.indexes, has_length(len(index_intersection)))
        for ri in r.indexes:
            result_index_id = str(ri.index)

            assert_that(ids[result_key_id].get(result_index_id), not_none())
            assert_that(ri.data, equal_to(ids[result_key_id][result_index_id]))

