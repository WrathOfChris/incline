import unittest
import logging
from incline.InclineDatastoreMemory import InclineDatastoreMemory
from incline.InclineTraceConsole import InclineTraceConsole
#import InclineDatastore
from InclineDatastore import TestDatastore

log = logging.getLogger('incline')
log.setLevel(logging.INFO)

TEST_TABLE="test-incline-memory"
TEST_REGION="us-west-2"
TEST_PREFIX="test-datastoreMemory"

class TestDatastoreMemory(TestDatastore):
    maxdiff = None

    @classmethod
    def setUpClass(cls):
        cls.ds = InclineDatastoreMemory(
                name=TEST_TABLE,
                region=TEST_REGION
                )
        # opentelemetry traces to console
        if __name__ == "__main__":
            cls.ds.trace = InclineTraceConsole()

    def test_003_dbtype(self):
        """  ensure tests run on the correct datastore type """
        self.assertEqual(self.ds.dbtype, "memory")

if __name__ == "__main__":
    unittest.main()