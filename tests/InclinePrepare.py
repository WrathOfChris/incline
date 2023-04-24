import unittest
import decimal
import time
import uuid
import incline.InclinePrepare

pxn = incline.InclinePrepare.InclinePrepare()


class TestInclinePrepare(unittest.TestCase):
    maxDiff = None

    def test_pxn(self):
        pass

    def test_cmppxn(self):
        pass

    def test_cid(self):
        self.assertEqual(pxn.cid(), incline.base62.base_encode(uuid.getnode()))

    def test_cnt(self):
        pass

    def test_now(self):
        """ Same tests as Datastore """
        now = pxn.now()
        now2 = pxn.now()
        now_time = time.time()

        self.assertIsInstance(now, decimal.Decimal)

        self.assertNotEqual(now, 0)
        self.assertNotEqual(now, now2)
        self.assertGreater(now2, now)

        # called later, must be greater
        self.assertGreater(now_time, now)

        # called later, must be greater
        now3 = decimal.Decimal(time.time_ns()) / 1000000000
        self.assertGreater(now3, now)

    def test_decimal(self):
        """ Same tests as Datastore """
        self.assertIsInstance(pxn.decimal(int(1)), decimal.Decimal)

        (s, d, e) = pxn.decimal(1).as_tuple()
        self.assertEqual(abs(e), 6)

        v = pxn.decimal("1.0000000001")
        (s, d, e) = v.as_tuple()
        self.assertEqual(abs(e), 6)
        self.assertEqual(v, decimal.Decimal("1.000000"))

        v = pxn.decimal("1.0000000000001")
        (s, d, e) = v.as_tuple()
        self.assertEqual(abs(e), 6)
        self.assertEqual(v, decimal.Decimal("1.000000"))

        v = pxn.decimal("1.1")
        (s, d, e) = v.as_tuple()
        self.assertEqual(abs(e), 6)
        self.assertEqual(v, decimal.Decimal("1.100000"))


if __name__ == "__main__":
    unittest.main()
