import unittest
from incline.InclineDatastoreDynamo import InclineDatastoreDynamo
from incline.InclineTraceConsole import InclineTraceConsole
from InclineDatastore import TestDatastore
import logging
import decimal
import sys
import botocore

log = logging.getLogger('incline')
log.setLevel(logging.INFO)

TEST_TABLE = "test-incline-dynamo"
TEST_REGION = "us-west-2"
TEST_PREFIX = "test-datastoreDynamo"


class TestDatastoreDynamo(TestDatastore):
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.ds = InclineDatastoreDynamo(name=TEST_TABLE, region=TEST_REGION)
        # opentelemetry traces to console
        if __name__ == "__main__":
            cls.ds.trace = InclineTraceConsole()

    def test_003_dbtype(self) -> None:
        """  ensure tests run on the correct datastore type """
        self.assertEqual(self.ds.dbtype, "dynamo")

#    def ds_get_log(self, kid, pxn=None) -> None:
#        pass
#
#    def ds_get_txn(self, kid, tsv=None, limit=1) -> None:
#        pass
#
#    def ds_prepare(self, kid, val) -> None:
#        pass
#
#    def ds_commit(self, kid, log, mode=None) -> None:
#        pass
#
#    def test_map_log_response(self) -> None:
#        pass
#
#    def test_map_txn_response(self) -> None:
#        pass
#
#    def test_map_log_response_v1(self) -> None:
#        pass
#
#    def test_map_txn_response_v1(self) -> None:
#        pass
#
## TODO check config of dynamo table

    def test_002_setup(self) -> None:
        """ catch expected ResourceInUseException """
        ids = InclineDatastoreDynamo(name=TEST_TABLE, region=TEST_REGION)
        try:
            ids.setup()
        except botocore.exceptions.ClientError as e:
            self.assertEqual(e.response['Error']['Code'],
                             'ResourceInUseException')


#    def test_ds_setup(self) -> None:
#        pass
#
#    def test_ds_setup_log(self) -> None:
#        pass
#
#    def test_ds_setup_txn(self) -> None:
#        pass

if __name__ == "__main__":
    unittest.main()
