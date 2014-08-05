import random

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest import has_properties, has_property, equal_to, greater_than_or_equal_to

from test_helper.utils import get_sha1

class WithSameSha1As(BaseMatcher):
    def __init__(self, data):
        self.expected_sha1 = get_sha1(data)

    def _matches(self, item):
        result_sha1 = get_sha1(str(item))
        return result_sha1 == self.expected_sha1

    def describe_to(self, description):
        description.append_text('by sha1 to ') \
                   .append_text(self.expected_sha1)

    def describe_mismatch(self, item, mismatch_description):
        mismatch_description.append_text(get_sha1(str(item)))

def with_same_sha1_as(data):
    """Matches if item has the same sha1 hash."""
    return WithSameSha1As(data)

def elliptics_result_with(error_code, timestamp, user_flags, data):
    """Matches if elliptics async_result meets the following conditions:
    
      * async_result.error.code == zero
      * async_result.timestamp >= timestamp before this operation
      * async_result.user_flags has proper value

    """
    return has_properties('error', has_property('code', equal_to(error_code)),
                          'timestamp', greater_than_or_equal_to(timestamp),
                          'user_flags', equal_to(user_flags),
                          'data', with_same_sha1_as(data))

class HasItems(BaseMatcher):
    def __init__(self, *elements):
        self.elements = elements
        self.diff = None

    def _matches(self, item):
        self.diff = set(self.elements).difference(set(item))
        return len(self.diff) == 0

    def describe_to(self, description):
        description.append_text("a sequence has the {0} elements".format(len(self.elements)))

    def describe_mismatch(self, item, mismatch_description):
        if len(self.diff) >= 3:
            sample = random.sample(self.diff, 3)
        else:
            sample = self.diff
        sample = map(str, sample)
        mismatch_description.append_text("got {0} elements difference ([{1}...])"\
                                             .format(len(self.diff), ', '.join(sample)))

def hasitems(*elements):
    """Custom has_items matcher with a short description."""
    return HasItems(*elements)

class RaisesEllipticsError(BaseMatcher):
    def __init__(self, expected, exc_code=None):
        self.exc_code = exc_code
        self.expected = expected
        self.mismatch_description = None

    def _matches(self, item):
        if not hasattr(item, '__call__'):
            self.mismatch_description = "{} is not callable".format(item)
            return False

        try:
            item()
            self.mismatch_description = "No exception raised."
        except self.expected as exc:
            if exc.message.code == self.exc_code:
                return True
            else:
                self.mismatch_description = "Correct exception raised, but the expected " \
                    "code ({}) not found.\ncode was: {}.".format(self.exc_code, exc.message.code)
        except Exception as exc:
            self.mismatch_description = "{} was raised instead".format(type(exc))
        return False

    def describe_to(self, description):
        description.append_text("the callable raising {}".format(self.expected))

    def describe_mismatch(self, item, description):
        description.append_text(self.mismatch_description)

def raises_elliptics_error(exception, exc_code):
    """Matches if the called function raised the expected exception with correct code."""
    return RaisesEllipticsError(exception, exc_code)
