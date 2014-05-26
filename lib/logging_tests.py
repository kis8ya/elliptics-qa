"""This module allows to add some output for tests
which should not be displayed in TeamCity build log.
"""
import os.path
import logging
import logging.config
import logging.handlers

class NoNewlineHandler(logging.StreamHandler):
    """Sends logging output to file-like objects
    (w/o newline symbol)
    """
    def emit(self, record):
        """Emits a record.
        Note: it has flushed output on each write.
        """
        msg = self.format(record)
        self.stream.write('%s' % msg)
        self.stream.flush()

# Register custom handler to be able to use it in logging configuration file
logging.handlers.NoNewlineHandler = NoNewlineHandler

_conf_file = os.path.dirname(__file__)
_conf_file = os.path.join(_conf_file, 'logger.ini')
logging.config.fileConfig(_conf_file)

logger = logging.getLogger('testLogger')
