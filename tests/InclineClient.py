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
        kid = f"{TEST_PREFIX}-create-{self.tsv}"
        dat = {'key': kid}
        resp = self.ramp.create(kid, dat)
        self.assertNotEqual(resp.pxn, InclinePxn())
        self.assertNotEqual(resp.only.tsv, 0, msg="create tsv cannot be zero")
        self.assertNotEqual(resp.only.ver, 0, msg="create ver cannot be zero")
        self.assertGreater(resp.only.tsv, self.tsv,
                           msg="create tsv must be after test run starts")

    def test_create_get(self) -> None:
        kid = f"{TEST_PREFIX}-create-get-{self.tsv}"
        dat = {'key': kid}

        resp = self.ramp.create(kid, dat)
        rec_1 = resp.only

        resp = self.ramp.get(kid)
        self.assertDictEqual(rec_1.to_dict(), resp.only.to_dict(),
                             msg="get after create should be equal")

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
        rec_1 = resp.only
        self.assertNotEqual(resp.pxn, InclinePxn())

        resp = self.ramp.delete(kid)
        rec_2 = resp.only
        self.assertNotEqual(resp.pxn, InclinePxn())

        resp = self.ramp.create(kid, dat)
        rec_3 = resp.only

        self.assertNotEqual(rec_1.tsv, rec_3.tsv,
                            msg="create after delete overwrote first")

        self.assertGreater(rec_3.tsv, rec_1.tsv,
                            msg="second create must be later")

        resp = self.ramp.get(kid)
        rec_4 = resp.only

        self.assertDictEqual(rec_3.to_dict(), rec_4.to_dict(),
                             msg="get must be second create")

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
        self.assertEqual(dict(value=self.tsv),
                         self.ramp.get(f"{TEST_PREFIX}-putget").only.data)

    def test_set_index(self) -> None:
        kid = f"{TEST_PREFIX}-set-index-{self.tsv}"
        index = incline.InclineDatastore.InclineIndex(name='one',
                                                      path='one')
        self.ramp.set_index(index)
        self.assertIn('one', self.ramp.indexes)
        resp = self.ramp.create(kid, {
            'one': {'two': 'three'},
            'four': 'five'
            })
        rec = resp.only
        self.assertIn('one', rec.idx)
        self.assertEqual({'two': 'three'}, rec.idx['one'].value)

    def test_set_index_path(self) -> None:
        kid = f"{TEST_PREFIX}-set-index-path-{self.tsv}"
        index = incline.InclineDatastore.InclineIndex(name='two',
                                                      path='one.two')
        self.ramp.set_index(index)
        self.assertIn('two', self.ramp.indexes)
        resp = self.ramp.create(kid, {
            'one': {'two': 'three'},
            'four': 'five'
            })
        rec = resp.only
        self.assertIn('two', rec.idx)
        self.assertEqual('three', rec.idx['two'].value)

    def test_set_index_value(self) -> None:
        kid = f"{TEST_PREFIX}-set-index-value-{self.tsv}"
        index = incline.InclineDatastore.InclineIndex(name='six',
                                                      value='seven')
        self.ramp.set_index(index)
        self.assertIn('six', self.ramp.indexes)
        resp = self.ramp.create(kid, {
            'one': {'two': 'three'},
            'four': 'five'
            })
        rec = resp.only
        self.assertIn('six', rec.idx)
        self.assertEqual('seven', rec.idx['six'].value)


    def test_set_index_value_float(self) -> None:
        kid = f"{TEST_PREFIX}-set-index-value-float-{self.tsv}"
        index = incline.InclineDatastore.InclineIndex(name='eight',
                                                      value=0.12345)
        self.ramp.set_index(index)
        resp = self.ramp.create(kid, {
            'one': {'two': 'three'},
            'four': 'five'
            })
        rec = resp.only
        self.assertIn('eight', rec.idx)
        self.assertEqual(Decimal('0.12345'), rec.idx['eight'].value)

    def test_set_index_path_float(self) -> None:
        kid = f"{TEST_PREFIX}-set-index-path-float-{self.tsv}"
        index = incline.InclineDatastore.InclineIndex(name='ten',
                                                      path='nine.ten')
        self.ramp.set_index(index)
        self.assertIn('two', self.ramp.indexes)
        resp = self.ramp.create(kid, {
            'one': {'two': 'three'},
            'four': 'five',
            'nine': {'ten': 11.424242}
            })
        rec = resp.only
        self.assertIn('ten', rec.idx)
        self.assertEqual(Decimal('11.424242'), rec.idx['ten'].value)

    def test_set_index_value_refresh(self) -> None:
        kid1 = f"{TEST_PREFIX}-set-index-value-refresh-1-{self.tsv}"
        kid2 = f"{TEST_PREFIX}-set-index-value-refresh-2-{self.tsv}"
        # reset indexes
        self.ramp.indexes = {}
        # create without index
        create_resp = self.ramp.creates([
            {'kid': kid1,
             'dat': {
                 'one': {'two': 'three'},
                 'four': 'five'
                 },
             },
            {'kid': kid2,
             'dat': {
                 'one': {'two': 'three'},
                 'four': 'five'
                 },
             }])
        # add index
        index = incline.InclineDatastore.InclineIndex(name='twelve12',
                                                      value='thirteen')
        self.ramp.set_index(index)
        refresh_resp = self.ramp.refresh(kid2)
        self.assertEqual(create_resp.data[kid2].to_dict(), refresh_resp.only.to_dict())

    def test_type_string(self) -> None:
        key = f"{TEST_PREFIX}-type-string"
        val = str("hello")
        resp = self.ramp.put(key, val)    # type: ignore
        self.assertIsNotNone(resp)
        self.assertEqual(val, self.ramp.get(key).only.data)

    def test_type_integer(self) -> None:
        key = f"{TEST_PREFIX}-type-integer"
        val = int(42)
        resp = self.ramp.put(key, val)    # type: ignore
        self.assertIsNotNone(resp)
        self.assertEqual(val, self.ramp.get(key).only.data)

    def test_type_float(self) -> None:
        key = f"{TEST_PREFIX}-type-float"
        val = float(42.424242)
        resp = self.ramp.put(key, val)    # type: ignore
        self.assertIsNotNone(resp)
        # float internally converted to Decimal
        self.assertEqual(Decimal('42.424242'), self.ramp.get(key).only.data)

    def test_type_dict(self) -> None:
        key = f"{TEST_PREFIX}-type-dict"
        val = dict(value="string")
        resp = self.ramp.put(key, val)
        self.assertIsNotNone(resp)
        self.assertEqual(val, self.ramp.get(key).only.data)

    def test_type_dict_float(self) -> None:
        key = f"{TEST_PREFIX}-type-dict-float"
        val = {
                'float': 42.424242
                }
        resp = self.ramp.put(key, val)
        self.assertIsNotNone(resp)
        self.assertEqual({'float': Decimal('42.424242')},
                          self.ramp.get(key).only.data)

    def test_type_list(self) -> None:
        key = f"{TEST_PREFIX}-type-list"
        val = list("abcdef")
        resp = self.ramp.put(key, val)    # type: ignore
        self.assertIsNotNone(resp)
        self.assertEqual(val, self.ramp.get(key).only.data)

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
            key = f"{TEST_PREFIX}-type-decimal"
            val = {'decimal': Decimal(d)}    # type: ignore
            self.ramp.put(key, val)
            self.assertEqual(val, self.ramp.get(key).only.data)


if __name__ == "__main__":
    unittest.main()
