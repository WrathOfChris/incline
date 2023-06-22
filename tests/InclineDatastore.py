import unittest
from decimal import Decimal
import logging
import time
from typing import Any, Type
import uuid
import incline.InclineDatastore
import incline.InclineClient
from incline.InclinePrepare import InclinePxn
from incline.InclineTraceConsole import InclineTraceConsole
from incline.error import InclineNotFound

log = logging.getLogger('incline')
log.setLevel(logging.INFO)

TEST_TABLE = "test-incline-none"
TEST_REGION = "us-west-2"
TEST_PREFIX = "test-datastore"


class InclineDatastoreTest(incline.InclineDatastore.InclineDatastore):
    """
    Override datastore methods with test fixtures
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # permit individual tests to be run by setting up store_log/store_txn
        super().__init__(*args, **kwargs)
        self.ds_setup()

    def ds_get_log(self,
                   kid: str,
                   pxn: InclinePxn | None = None) -> list[dict[str, Any]]:
        logs = self.store_log.get(kid)
        if not logs:
            return []
        if not pxn:
            pxn = InclinePxn().loads(max(logs))
        log = logs.get(pxn.pxn)
        if not log:
            raise ValueError(f"{kid} {format(pxn)} not found")
        return [log]

    def ds_get_txn(self,
                   kid: str,
                   tsv: Decimal | None = None,
                   limit: int = 1) -> list[dict[str, Any]]:
        txns = self.store_txn.get(kid)
        if not txns:
            return []
        if not tsv:
            tsv = max(txns)
        txn = txns.get(tsv)
        if not txn:
            raise ValueError(f"{kid} {tsv} not found")
        return [txn]

    def ds_prepare(self, kid: str, val: dict[str,
                                             Any]) -> list[dict[str, Any]]:
        if kid not in self.store_log:
            self.store_log[kid] = {}
        self.store_log[kid][val.get('pxn')] = val
        return [val]

    def ds_commit(self,
                  kid: str,
                  log: dict[str, Any],
                  mode: str | None = None) -> list[dict[str, Any]]:
        """ fixture with no origin tsv (new item) """
        if kid not in self.store_txn:
            self.store_txn[kid] = {}
        txn = self.gentxn(log, tsv=0)
        self.store_txn[kid][txn.get('tsv')] = txn
        return [txn]

    def ds_scan_log(self,
                    kid: str | None = None,
                    tsv: Decimal | None = None,
                    limit: int | None = None) -> list[dict[str, Any]]:
        logs = list()
        for k, v in self.store_log.items():
            for p in v.keys():
                logs.append({'kid': k, 'pxn': p})
        return logs

    def ds_scan_txn(self,
                    kid: str | None = None,
                    tsv: Decimal | int | str | None = None,
                    limit: int | None = None) -> list[dict[str, Any]]:
        txns = list()
        for k, v in self.store_txn.items():
            for t in v.keys():
                txns.append({'kid': k, 'tsv': t})
        return txns

    def ds_delete_log(self, kid: str, pxn: InclinePxn) -> None:
        del self.store_log[kid][pxn.pxn]
        if not self.store_log[kid]:
            del self.store_log[kid]

    def ds_delete_txn(self, kid: str, tsv: Decimal) -> None:
        del self.store_txn[kid][tsv]
        if not self.store_txn[kid]:
            del self.store_txn[kid]

    def ds_setup(self) -> None:
        kid = f"{TEST_PREFIX}-prepare"
        pxn = InclinePxn(cid=0, cnt=0)
        self.store_log: dict[str, Any] = {}
        log = {
            'kid': kid,
            'pxn': pxn.pxn,
            'tsv': self.pxn.now(),
            'cid': self.pxn.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'ver': self.version,
            'met': [],    # TODO: self.canon_metadata()?
            'dat': "test fixture",
        }
        self.store_log[kid] = {}
        self.store_log[kid][pxn.pxn] = log

        self.store_txn: dict[str, Any] = {}
        kid = f"{TEST_PREFIX}-commit"
        tsv = self.pxn.now()
        txn = {
            'kid': kid,
            'tsv': tsv,
            'pxn': pxn.pxn,
            'tmb': False,
            'cid': self.pxn.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'org': 0,
            'ver': self.version,
            'met': [],    # TODO: self.canon_metadata()?
            'dat': "test fixture",
        }
        self.store_txn[kid] = {}
        self.store_txn[kid][tsv] = txn


ramp = incline.InclineClient.InclineClient(
    name=TEST_TABLE,
    region=TEST_REGION,
    rid='123e4567-e89b-12d3-a456-426655440000',
    uid='00000000-0000-0000-0000-000000000000')


class TestDatastore(unittest.TestCase):
    maxDiff = None
    ds: incline.InclineDatastore.InclineDatastore
    test_prepare_commit_pxn: InclinePxn

    @classmethod
    def setUpClass(cls) -> None:
        cls.ds = InclineDatastoreTest(name=TEST_TABLE, region=TEST_REGION)
        # opentelemetry traces to console
        if __name__ == "__main__":
            cls.ds.trace = InclineTraceConsole()

    def test_002_setup(self) -> None:
        self.ds.ds_setup()

    def test_003_dbtype(self) -> None:
        """  ensure tests run on the correct datastore type """
        self.assertEqual(self.ds.dbtype, "none")

    def test_004_fixtures(self) -> None:
        """ bypass datastore prepare() """
        kid = f"{TEST_PREFIX}-log-key-no-pxn"
        pxn = ramp.prepare.pxn()
        dat = {'kid': kid, 'dat': kid}
        met = ramp.genmet([], "", kid, pxn, [dat])
        val = self.ds.prepare_val(kid, pxn, met, dat)
        resp = self.ds.ds_prepare(kid, val)

    def test_zzz_scan_clean_log(self) -> None:
        logs = self.ds.ds_scan_log()
        self.assertNotEqual(logs, [])
        print(logs)

        if logs:
            for l in logs:
                self.ds.ds_delete_log(l['kid'], InclinePxn().loads(l['pxn']))

        logs = self.ds.ds_scan_log()
        self.assertEqual(logs, [])

    def test_zzz_scan_clean_txn(self) -> None:
        txns = self.ds.ds_scan_txn()
        self.assertNotEqual(txns, [])
        print(txns)

        if txns:
            for t in txns:
                self.ds.ds_delete_txn(t['kid'], t['tsv'])

        txns = self.ds.ds_scan_txn()
        self.assertEqual(txns, [])

    def test_uid_none(self) -> None:
        self.assertEqual(self.ds.uid(), '0')

    def test_rid_none(self) -> None:
        self.assertEqual(self.ds.rid(), '0')

    def test_numbers_to_remote_values(self) -> None:
        self.assertIsInstance(self.ds.numbers_to_remote(int(1)), int)
        self.assertIsInstance(self.ds.numbers_to_remote(float(1.0)), Decimal)

    def test_numbers_to_remote_list(self) -> None:
        v = self.ds.numbers_to_remote([int(1), float(1.0)])
        self.assertIsInstance(v[0], int)
        self.assertIsInstance(v[1], Decimal)

    def test_numbers_to_remote_dict(self) -> None:
        v = self.ds.numbers_to_remote({'i': int(1), 'f': float(1.0)})
        self.assertIsInstance(v['i'], int)
        self.assertIsInstance(v['f'], Decimal)

    def test_numbers_to_local_values(self) -> None:
        self.assertIsInstance(self.ds.numbers_to_local(Decimal(1)), int)
        self.assertIsInstance(self.ds.numbers_to_local(Decimal("1.0")), float)

    def test_numbers_to_local_list(self) -> None:
        v = self.ds.numbers_to_local([Decimal(1), Decimal("1.0")])
        self.assertIsInstance(v[0], int)
        self.assertIsInstance(v[1], float)

    def test_numbers_to_local_dict(self) -> None:
        v = self.ds.numbers_to_local({'i': Decimal(1), 'f': Decimal("1.0")})
        self.assertIsInstance(v['i'], int)
        self.assertIsInstance(v['f'], float)

    def test_prepare_val(self) -> None:
        kid = f"{TEST_PREFIX}-prepare"
        pxn = ramp.prepare.pxn()
        dat = {'kid': kid, 'dat': kid}
        met = ramp.genmet([], "", kid, pxn, [dat])
        p = self.ds.prepare_val(kid, pxn, met, dat)
        self.assertEqual(p['kid'], kid)
        self.assertEqual(p['pxn'], pxn.pxn)
        self.assertEqual(p['cid'], self.ds.pxn.cid())
        self.assertEqual(p['dat'], dat)
        self.assertEqual(p['met'], met.to_dict())
        self.assertEqual(p['rid'], self.ds.rid())
        self.assertEqual(p['uid'], self.ds.uid())
        self.assertIsInstance(p['tsv'], Decimal)
        self.assertGreater(self.ds.pxn.now(), p['tsv'])

    def fixture(self,
                kid: str,
                dat: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        pxn = ramp.prepare.pxn()
        datseq = []
        if dat:
            datseq.append(dat)
        else:
            dat = {}
        met = ramp.genmet([], "", kid, pxn, datseq)
        prepare = self.ds.prepare(kid, pxn, met, dat)
        commit = self.ds.commit(kid, pxn)
        return commit

    def test_prepare_commit_1(self) -> None:
        kid = f"{TEST_PREFIX}-prepare-commit"
        pxn = ramp.prepare.pxn()
        # store value for next test
        self.__class__.test_prepare_commit_pxn = pxn
        dat = {'kid': kid, 'dat': kid}
        met = ramp.genmet([], "", kid, pxn, [dat])
        prepare = self.ds.prepare(kid, pxn, met, dat)
        p = self.ds.only(prepare)
        self.assertEqual(p['kid'], kid)
        self.assertEqual(p['pxn'], pxn.pxn)
        self.assertEqual(p['cid'], self.ds.pxn.cid())
        self.assertEqual(p['met'], met.to_dict())
        self.assertEqual(p['rid'], self.ds.rid())
        self.assertEqual(p['uid'], self.ds.uid())
        self.assertIsInstance(p['tsv'], Decimal)
        self.assertGreater(self.ds.pxn.now(), p['tsv'])
        self.assertEqual(p['dat'], dat)
        self.assertEqual(p['ver'], self.ds.version)

    def test_prepare_commit_2(self) -> None:
        kid = f"{TEST_PREFIX}-prepare-commit"
        # retrive value from previous test
        pxn = self.__class__.test_prepare_commit_pxn
        commit = self.ds.commit(kid, pxn)
        c = self.ds.only(commit)
        self.assertEqual(c['kid'], kid)
        self.assertEqual(c['pxn'], pxn.pxn)
        self.assertEqual(c['cid'], self.ds.pxn.cid())
        self.assertEqual(c['met'], [])
        self.assertEqual(c['rid'], self.ds.rid())
        self.assertEqual(c['uid'], self.ds.uid())
        self.assertIsInstance(c['tsv'], Decimal)
        self.assertGreater(self.ds.pxn.now(), c['tsv'])
        self.assertEqual(c['dat'], {'kid': kid, 'dat': kid})
        self.assertFalse(c['tmb'])
        self.assertEqual(c['ver'], self.ds.version)

        # test and memory guarantee this is new record.  persistent does not
        if self.ds.dbtype in ['none', 'memory']:
            self.assertEqual(c['org'], 0)

    def test_ds_get_txn_kid_notfound(self) -> None:
        kid = f"{TEST_PREFIX}-never-store-this"
        resp = self.ds.ds_get_txn(kid, tsv=None)
        self.assertEqual(resp, [])

    def test_ds_get_txn_txn_notfound(self) -> None:
        kid = f"{TEST_PREFIX}-never-store-this"
        resp = self.ds.ds_get_txn(kid, tsv=Decimal(42))
        self.assertEqual(resp, [])

    def test_delete_tombstone(self) -> None:
        kid = f"{TEST_PREFIX}-delete-tombstone"
        fix = self.ds.only(self.fixture(kid, None))
        resp = self.ds.only(self.ds.ds_get_txn(kid, tsv=fix['tsv']))
        self.assertGreater(resp['tmb'], 0)
        self.assertEqual(resp['dat'], {})

    def test_prepare_commit_create(self) -> None:
        pass

    def test_prepare_commit_update(self) -> None:
        # needs a fixture
        pass


if __name__ == "__main__":
    unittest.main()
