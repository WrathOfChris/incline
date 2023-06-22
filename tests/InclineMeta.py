import unittest
from incline.InclineMeta import InclineMeta, InclineMetaWrite
from incline.InclinePrepare import InclinePxn

class TestInclineMeta(unittest.TestCase):
    maxDiff = None

    def test_meta(self):
        meta = InclineMeta()
        self.assertEqual(meta.meta, [])

    def test_meta_to_dict(self):
        meta = InclineMeta()
        write = InclineMetaWrite("1", "", "")
        meta.add_write(write)

        metadict = meta.to_dict()
        self.assertEqual(metadict, [{'kid': '1', 'loc': '', 'pxn': ''}])

    def test_metawrite(self):
        write = InclineMetaWrite("1", "2")
        self.assertEqual(write.kid, "1")
        self.assertEqual(write.loc, "2")
        self.assertEqual(write.pxn, InclinePxn())

    def test_metawrite_all(self):
        write = InclineMetaWrite("1", "2", "3")
        self.assertEqual(write.kid, "1")
        self.assertEqual(write.loc, "2")
        self.assertEqual(write.pxn, "3")

    def test_metawrite_to_dict(self):
        write = InclineMetaWrite("1", "2", "3")

        writedict = write.to_dict()
        self.assertEqual(writedict, {'kid': '1', 'loc': '2', 'pxn': '3'})
