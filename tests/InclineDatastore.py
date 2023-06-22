import unittest
import decimal
import logging
import time
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

    def __init__(self, *args, **kwargs):
        # permit individual tests to be run by setting up store_log/store_txn
        super().__init__(*args, **kwargs)
        self.ds_setup()

    def ds_get_log(self, kid, pxn=None):
        logs = self.store_log.get(kid)
        if not logs:
            return []
        if not pxn:
            pxn = max(logs)
        log = logs.get(pxn.pxn)
        if not log:
            raise ValueError(f"{kid} {format(pxn)} not found")
        return [log]

    def ds_get_txn(self, kid, tsv=None, limit=1):
        txns = self.store_txn.get(kid)
        if not txns:
            return []
        if not tsv:
            tsv = max(txns)
        txn = txns.get(tsv)
        if not txn:
            raise ValueError(f"{kid} {tsv} not found")
        return [txn]

    def ds_prepare(self, kid, val):
        if kid not in self.store_log:
            self.store_log[kid] = {}
        self.store_log[kid][val.get('pxn')] = val
        return [val]

    def ds_commit(self, kid, log, mode=None):
        """ fixture with no origin tsv (new item) """
        if kid not in self.store_txn:
            self.store_txn[kid] = {}
        txn = self.gentxn(log, tsv=0)
        self.store_txn[kid][txn.get('tsv')] = txn
        return [txn]

    def ds_scan_log(self, kid=None, tsv=None, limit=None):
        logs = list()
        for k, v in self.store_log.items():
            for p in v.keys():
                logs.append({'kid': k, 'pxn': p})
        return logs

    def ds_scan_txn(self, kid=None, tsv=None, limit=None):
        txns = list()
        for k, v in self.store_txn.items():
            for t in v.keys():
                txns.append({'kid': k, 'tsv': t})
        return txns

    def ds_delete_log(self, kid, pxn):
        del self.store_log[kid][pxn]
        if not self.store_log[kid]:
            del self.store_log[kid]

    def ds_delete_txn(self, kid, tsv):
        del self.store_txn[kid][tsv]
        if not self.store_txn[kid]:
            del self.store_txn[kid]

    def ds_setup(self):
        kid = f"{TEST_PREFIX}-prepare"
        pxn = InclinePxn(cid=0, cnt=0)
        self.store_log = {}
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

        self.store_txn = {}
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

    @classmethod
    def setUpClass(cls):
        cls.ds = InclineDatastoreTest(name=TEST_TABLE, region=TEST_REGION)
        # opentelemetry traces to console
        if __name__ == "__main__":
            cls.ds.trace = InclineTraceConsole()

    def test_002_setup(self):
        self.ds.ds_setup()

    def test_003_dbtype(self):
        """  ensure tests run on the correct datastore type """
        self.assertEqual(self.ds.dbtype, "none")

    def test_004_fixtures(self):
        """ bypass datastore prepare() """
        kid = f"{TEST_PREFIX}-log-key-no-pxn"
        pxn = ramp.prepare.pxn()
        dat = [{'kid': kid, 'dat': kid}]
        met = ramp.genmet([], None, kid, pxn, dat)
        val = self.ds.prepare_val(kid, pxn, met, dat)
        resp = self.ds.ds_prepare(kid, val)

    def test_zzz_scan_clean_log(self):
        logs = self.ds.ds_scan_log()
        self.assertNotEqual(logs, [])
        print(logs)

        if logs:
            for l in logs:
                self.ds.ds_delete_log(l['kid'], l['pxn'])

        logs = self.ds.ds_scan_log()
        self.assertEqual(logs, [])

    def test_zzz_scan_clean_txn(self):
        txns = self.ds.ds_scan_txn()
        self.assertNotEqual(txns, [])
        print(txns)

        if txns:
            for t in txns:
                self.ds.ds_delete_txn(t['kid'], t['tsv'])

        txns = self.ds.ds_scan_txn()
        self.assertEqual(txns, [])

    def test_uid_none(self):
        self.assertEqual(self.ds.uid(), '0')

    def test_rid_none(self):
        self.assertEqual(self.ds.rid(), '0')

    def test_numbers_to_remote_values(self):
        self.assertIsInstance(self.ds.numbers_to_remote(int(1)), int)
        self.assertIsInstance(self.ds.numbers_to_remote(float(1.0)),
                              decimal.Decimal)

    def test_numbers_to_remote_list(self):
        v = self.ds.numbers_to_remote([int(1), float(1.0)])
        self.assertIsInstance(v[0], int)
        self.assertIsInstance(v[1], decimal.Decimal)

    def test_numbers_to_remote_dict(self):
        v = self.ds.numbers_to_remote({'i': int(1), 'f': float(1.0)})
        self.assertIsInstance(v['i'], int)
        self.assertIsInstance(v['f'], decimal.Decimal)

    def test_numbers_to_local_values(self):
        self.assertIsInstance(self.ds.numbers_to_local(decimal.Decimal(1)),
                              int)
        self.assertIsInstance(self.ds.numbers_to_local(decimal.Decimal("1.0")),
                              float)

    def test_numbers_to_local_list(self):
        v = self.ds.numbers_to_local(
            [decimal.Decimal(1), decimal.Decimal("1.0")])
        self.assertIsInstance(v[0], int)
        self.assertIsInstance(v[1], float)

    def test_numbers_to_local_dict(self):
        v = self.ds.numbers_to_local({
            'i': decimal.Decimal(1),
            'f': decimal.Decimal("1.0")
        })
        self.assertIsInstance(v['i'], int)
        self.assertIsInstance(v['f'], float)

    def test_prepare_val(self):
        kid = f"{TEST_PREFIX}-prepare"
        pxn = ramp.prepare.pxn()
        dat = [{'kid': kid, 'dat': kid}]
        met = ramp.genmet([], None, kid, pxn, dat)
        p = self.ds.prepare_val(kid, pxn, met, dat)
        self.assertEqual(p['kid'], kid)
        self.assertEqual(p['pxn'], pxn.pxn)
        self.assertEqual(p['cid'], self.ds.pxn.cid())
        self.assertEqual(p['dat'], dat)
        self.assertEqual(p['met'], met.to_dict())
        self.assertEqual(p['rid'], self.ds.rid())
        self.assertEqual(p['uid'], self.ds.uid())
        self.assertIsInstance(p['tsv'], decimal.Decimal)
        self.assertGreater(self.ds.pxn.now(), p['tsv'])

    def fixture(self, kid, dat=None):
        pxn = ramp.prepare.pxn()
        met = ramp.genmet([], None, kid, pxn, dat)
        prepare = self.ds.prepare(kid, pxn, met, dat)
        commit = self.ds.commit(kid, pxn)
        return commit

    def test_prepare_commit_1(self):
        kid = f"{TEST_PREFIX}-prepare-commit"
        pxn = ramp.prepare.pxn()
        # store value for next test
        self.__class__.test_prepare_commit_pxn = pxn
        dat = [{'kid': kid, 'dat': kid}]
        met = ramp.genmet([], None, kid, pxn, dat)
        prepare = self.ds.prepare(kid, pxn, met, dat)
        p = self.ds.only(prepare)
        self.assertEqual(p['kid'], kid)
        self.assertEqual(p['pxn'], pxn.pxn)
        self.assertEqual(p['cid'], self.ds.pxn.cid())
        self.assertEqual(p['met'], met.to_dict())
        self.assertEqual(p['rid'], self.ds.rid())
        self.assertEqual(p['uid'], self.ds.uid())
        self.assertIsInstance(p['tsv'], decimal.Decimal)
        self.assertGreater(self.ds.pxn.now(), p['tsv'])
        self.assertEqual(p['dat'], dat)
        self.assertEqual(p['ver'], self.ds.version)

    def test_prepare_commit_2(self):
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
        self.assertIsInstance(c['tsv'], decimal.Decimal)
        self.assertGreater(self.ds.pxn.now(), c['tsv'])
        self.assertEqual(c['dat'], [{'kid': kid, 'dat': kid}])
        self.assertFalse(c['tmb'])
        self.assertEqual(c['ver'], self.ds.version)

        # test and memory guarantee this is new record.  persistent does not
        if self.ds.dbtype in ['none', 'memory']:
            self.assertEqual(c['org'], 0)

    def test_ds_get_txn_kid_notfound(self):
        kid = f"{TEST_PREFIX}-never-store-this"
        resp = self.ds.ds_get_txn(kid, tsv=None)
        self.assertEqual(resp, [])

    def test_ds_get_txn_txn_notfound(self):
        kid = f"{TEST_PREFIX}-never-store-this"
        resp = self.ds.ds_get_txn(kid, tsv=42)
        self.assertEqual(resp, [])

    def test_delete_tombstone(self):
        kid = f"{TEST_PREFIX}-delete-tombstone"
        fix = self.ds.only(self.fixture(kid, None))
        resp = self.ds.only(self.ds.ds_get_txn(kid, tsv=fix['tsv']))
        self.assertGreater(resp['tmb'], 0)
        self.assertEqual(resp['dat'], None)

    def test_prepare_commit_create(self):
        pass

    def test_prepare_commit_update(self):
        # needs a fixture
        pass


if __name__ == "__main__":
    unittest.main()
