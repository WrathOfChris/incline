import unittest
from decimal import Decimal
import logging

import incline.InclineClient
from incline.InclinePrepare import InclinePxn
from incline.InclineTraceConsole import InclineTraceConsole
import incline.InclineTraceConsole
from incline.error import InclineExists, InclineNotFound

log = logging.getLogger('incline')
log.setLevel(logging.INFO)

TEST_TABLE = "test-incline"
TEST_REGION = "us-west-2"
TEST_PREFIX = "test-InclineClient"


class TestInclineClient(unittest.TestCase):
    maxDiff = None
    ramp: incline.InclineClient.InclineClient
    tsv: Decimal

    @classmethod
    def setUpClass(cls) -> None:
        cls.ramp = incline.InclineClient.InclineClient(
            name=TEST_TABLE,
            region=TEST_REGION,
            rid='123e4567-e89b-12d3-a456-426655440000',
            uid='00000000-0000-0000-0000-000000000000')

        # fixtures
        cls.tsv = cls.ramp.prepare.now()

        # opentelemetry traces to console
        if __name__ == "__main__":
            cls.ramp.trace = InclineTraceConsole()

    def test_get(self) -> None:
        pass

    def test_get_notfound(self) -> None:
        kid = f"{TEST_PREFIX}-never-store-this"
        with self.assertRaises(InclineNotFound):
            items = self.ramp.get(kid)

    def test_put(self) -> None:
        pass

    def test_puts(self) -> None:
        pass

    def test_search(self) -> None:
        pass

    def test_create(self) -> None:
        pass

    def test_create_twice(self) -> None:
        kid = f"{TEST_PREFIX}-create-twice-{self.tsv}"
        dat = {'key': kid}
        resp = self.ramp.create(kid, dat)
        self.assertNotEqual(resp.pxn, InclinePxn())
        with self.assertRaises(InclineExists):
            resp = self.ramp.create(kid, dat)

    def test_create_delete_create(self) -> None:
        kid = f"{TEST_PREFIX}-create-delete-create-{self.tsv}"
        dat = {'key': kid}
        resp = self.ramp.create(kid, dat)
        self.assertNotEqual(resp.pxn, InclinePxn())
        resp = self.ramp.delete(kid)
        self.assertNotEqual(resp.pxn, InclinePxn())
        resp = self.ramp.create(kid, dat)
        self.assertNotEqual(resp.pxn,
                            InclinePxn(),
                            msg="key should not exist after previous delete")

    def test_creates(self) -> None:
        pass

    def test_delete(self) -> None:
        kid = f"{TEST_PREFIX}-delete"
        dat = {'key': kid}
        resp = self.ramp.create(kid, dat)
        self.assertNotEqual(resp.pxn, InclinePxn())
        resp = self.ramp.delete(kid)
        self.assertNotEqual(resp.pxn, InclinePxn())
        with self.assertRaises(InclineNotFound):
            items = self.ramp.get(kid)

    def test_getkey(self) -> None:
        pass

    def test_getlog(self) -> None:
        pass

    def test_putatomic(self) -> None:
        pass

    def test_genmet(self) -> None:
        pass

    def test_cmpval(self) -> None:
        pass

    def test_verify(self) -> None:
        pass

    def test_strval(self) -> None:
        pass

    def test_ds_find(self) -> None:
        pass

    def test_ds_equal(self) -> None:
        pass

    def test_ds_open(self) -> None:
        pass

    def test_putget_1(self) -> None:
        self.ramp.put(f"{TEST_PREFIX}-putget", dict(value=self.tsv))

    def test_putget_2(self) -> None:
        self.assertEqual(
            dict(value=self.tsv),
            self.ramp.get(f"{TEST_PREFIX}-putget").get(f"{TEST_PREFIX}-putget",
                                                       {}).get('dat'))

    def test_type_string(self) -> None:
        self.assertIsNotNone(
            self.ramp.put(f"{TEST_PREFIX}-type-string",
                          str("hello")))    # type: ignore

    def test_type_integer(self) -> None:
        self.assertIsNotNone(
            self.ramp.put(f"{TEST_PREFIX}-type-integer",
                          int(42)))    # type: ignore

    def test_type_float(self) -> None:
        self.assertIsNotNone(
            self.ramp.put(f"{TEST_PREFIX}-type-float",
                          float(42.424242)))    # type: ignore

    def test_type_dict(self) -> None:
        self.assertIsNotNone(
            self.ramp.put(f"{TEST_PREFIX}-type-dict", dict(value="string")))

    def test_type_list(self) -> None:
        self.assertIsNotNone(
            self.ramp.put(f"{TEST_PREFIX}-type-list",
                          list("abcdef")))    # type: ignore

    def test_type_decimal(self) -> None:
        DECIMAL_VALUES = [
            "1",
            "1.0",
            "1.000001",
            "1.000000001",
            "1.000000000001",
            2**31 - 1,
            2**63 - 1,
            2**32,
            2**64,
            2**32 + 1,
            2**64 + 1,
            10**38 - 1,    # dynamodb maximum size, 38 digits
            -10**38 + 1    # dynamodb minimum size, 38 digits
        ]
        for d in DECIMAL_VALUES:
            self.ramp.put(f"{TEST_PREFIX}-type-decimal",
                          Decimal(d))    # type: ignore
            self.assertEqual(
                Decimal(d),    # type: ignore
                self.ramp.get(f"{TEST_PREFIX}-type-decimal").get(
                    f"{TEST_PREFIX}-type-decimal", {}).get('dat'))


if __name__ == "__main__":
    unittest.main()
