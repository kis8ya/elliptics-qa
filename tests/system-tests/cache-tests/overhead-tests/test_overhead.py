import elliptics

from logging_tests import logger

def test_cache_overhead(client):
    """Testing that elliptics will process commands just in time
    when there is a cache overhead.
    """
    count = 100000
    logger.info("\n0/{0}".format(count))
    for i in xrange(count):
        key = str(i)
        try:
            # check that command will not raise elliptics.TimeoutError
            client.write_data_sync(key, '?')
        except elliptics.TimeoutError as e:
            assert e is None, e
        logger.info('\r{0}/{1}'.format(i + 1, count))
