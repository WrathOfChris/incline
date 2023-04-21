import unittest
import decimal
import logging

import incline.InclineClient
from incline.InclineTraceConsole import InclineTraceConsole
import incline.InclineTraceConsole

log = logging.getLogger('incline')
log.setLevel(logging.INFO)

TEST_TABLE="test-incline"
TEST_REGION="us-west-2"
TEST_PREFIX="test-InclineClient"

ramp = incline.InclineClient.InclineClient(
        name=TEST_TABLE,
        region=TEST_REGION,
        rid='123e4567-e89b-12d3-a456-426655440000',
        uid='00000000-0000-0000-0000-000000000000'
        )

# fixtures
tsv = ramp.pxn.now()

class TestInclineClient(unittest.TestCase):
    maxDiff = None

    def test_get(self):
        pass

    def test_put(self):
        pass

    def test_puts(self):
        pass

    def test_search(self):
        pass

    def test_create(self):
        pass

    def test_creates(self):
        pass

    def test_getkey(self):
        pass

    def test_getlog(self):
        pass

    def test_putatomic(self):
        pass

    def test_genmet(self):
        pass

    def test_cmpval(self):
        pass

    def test_verify(self):
        pass

    def test_strval(self):
        pass

    def test_ds_find(self):
        pass

    def test_ds_equal(self):
        pass

    def test_ds_open(self):
        pass

    def test_putget_1(self):
        ramp.put(f"{TEST_PREFIX}-putget", dict(value=tsv))

    def test_putget_2(self):
        self.assertEqual(dict(value=tsv),
                         ramp.get(f"{TEST_PREFIX}-putget").get(
                             f"{TEST_PREFIX}-putget").get('dat'))

    def test_type_string(self):
        self.assertIsNotNone(ramp.put(f"{TEST_PREFIX}-type-string",
                                      str("hello")))

    def test_type_integer(self):
        self.assertIsNotNone(ramp.put(f"{TEST_PREFIX}-type-integer",
                                      int(42)))

    def test_type_float(self):
        self.assertIsNotNone(ramp.put(f"{TEST_PREFIX}-type-float",
                                      float(42.424242)))

    def test_type_dict(self):
        self.assertIsNotNone(ramp.put(f"{TEST_PREFIX}-type-dict",
                                      dict(value="string")))

    def test_type_list(self):
        self.assertIsNotNone(ramp.put(f"{TEST_PREFIX}-type-list",
                                      list("abcdef")))

    def test_type_decimal(self):
        DECIMAL_VALUES = [
                "1",
                "1.0",
                "1.000001",
                "1.000000001",
                "1.000000000001",
                2**31-1,
                2**63-1,
                2**32,
                2**64,
                2**32+1,
                2**64+1,
                10**38-1,   # dynamodb maximum size, 38 digits
                -10**38+1   # dynamodb minimum size, 38 digits
                ]
        for d in DECIMAL_VALUES:
            ramp.put(f"{TEST_PREFIX}-type-decimal", decimal.Decimal(d))
            self.assertEqual(decimal.Decimal(d),
                             ramp.get(f"{TEST_PREFIX}-type-decimal").get(
                                 f"{TEST_PREFIX}-type-decimal").get('dat'))


if __name__ == "__main__":
    # opentelemetry traces to console
    ramp.trace = InclineTraceConsole()

    unittest.main()
