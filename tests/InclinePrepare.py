import unittest
from decimal import Decimal
import time
import uuid
import incline.InclinePrepare
from incline.base62 import base_encode

pxn = incline.InclinePrepare.InclinePrepare()


class TestInclinePrepare(unittest.TestCase):
    maxDiff = None

    def test_cid_just(self) -> None:
        self.assertEqual(incline.InclinePrepare.INCLINE_TXN_CID_JUST,
                         len(base_encode(2**48)))

    def test_cnt_just(self) -> None:
        self.assertEqual(incline.InclinePrepare.INCLINE_TXN_CNT_JUST,
                         len(base_encode(2**64)))

    def test_pxn(self) -> None:
        p = incline.InclinePrepare.InclinePxn(cid=0)
        self.assertEqual(p.pxn, "000000000.00000000000")
        self.assertEqual(p.pxn, str(p))
        self.assertEqual(p.pxn, format(p))
        self.assertEqual("InclinePxn(cnt=0, cid=0)", repr(p))

        p2 = incline.InclinePrepare.InclinePxn()
        cid = base_encode(incline.InclinePrepare.INCLINE_TXN_CLIENTID).rjust(
            9, '0')
        self.assertNotEqual(p2.pxn, "000000000.00000000000")
        self.assertEqual(p2.pxn, f"{cid}.00000000000")

    def test_pxn_order(self) -> None:
        p0 = incline.InclinePrepare.InclinePxn(cid=0, cnt=10)
        p1 = incline.InclinePrepare.InclinePxn(cid=0, cnt=20)
        p2 = incline.InclinePrepare.InclinePxn(cid=100, cnt=10)
        p3 = incline.InclinePrepare.InclinePxn(cid=100, cnt=20)
        p4 = incline.InclinePrepare.InclinePxn(cid=200, cnt=10)
        p5 = incline.InclinePrepare.InclinePxn(cid=0, cnt=10)

        # cid & cnt equal
        self.assertEqual(p0, p5)

        # cnt greater
        self.assertGreater(p1, p0)
        self.assertGreater(p1, p2)

        # cid greater, same cnt
        self.assertGreater(p4, p2)
        self.assertGreater(p3, p4)

    def test_pxn_loads(self) -> None:
        p1 = incline.InclinePrepare.InclinePxn().loads("000000000.00000000000")
        self.assertEqual(p1.cid, 0)
        self.assertEqual(p1.cnt, 0)

        p2 = incline.InclinePrepare.InclinePxn().loads("0ryIfPzwQ.00000000000")
        self.assertEqual(p2.cid, 190070690681122)
        self.assertEqual(p2.cnt, 0)

    def test_cid(self) -> None:
        self.assertEqual(pxn.cid(), base_encode(uuid.getnode()).rjust(9, '0'))

    def test_cnt(self) -> None:
        pass

    def test_quantize(self) -> None:
        self.assertEqual(incline.InclinePrepare.INCLINE_TXN_QUANTIZE,
                         "1.000000")

    def test_monotize(self) -> None:
        self.assertEqual(incline.InclinePrepare.INCLINE_TXN_MONOTIZE,
                         "0.000001")
        self.assertEqual(
            Decimal(incline.InclinePrepare.INCLINE_TXN_MONOTIZE) * 1000000,
            Decimal(incline.InclinePrepare.INCLINE_TXN_QUANTIZE))

    def test_now(self) -> None:
        """ Same tests as Datastore """
        now = pxn.now()
        now2 = pxn.now()
        now_time = time.time()

        self.assertIsInstance(now, Decimal)

        self.assertNotEqual(now, 0)
        self.assertNotEqual(now, now2)
        self.assertGreater(now2, now)

        # called later, must be greater
        self.assertGreater(now_time, now)

        # called later, must be greater
        now3 = Decimal(time.time_ns()) / 1000000000
        self.assertGreater(now3, now)

    def test_decimal(self) -> None:
        """ Same tests as Datastore """
        self.assertIsInstance(pxn.decimal(int(1)), Decimal)

        (s, d, e) = pxn.decimal(1).as_tuple()
        self.assertNotIn(e, ['n', 'N', 'F'])
        self.assertEqual(abs(int(e)), 6)

        v = pxn.decimal("1.0000000001")
        (s, d, e) = v.as_tuple()
        self.assertNotIn(e, ['n', 'N', 'F'])
        self.assertEqual(abs(int(e)), 6)
        self.assertEqual(v, Decimal("1.000000"))

        v = pxn.decimal("1.0000000000001")
        (s, d, e) = v.as_tuple()
        self.assertNotIn(e, ['n', 'N', 'F'])
        self.assertEqual(abs(int(e)), 6)
        self.assertEqual(v, Decimal("1.000000"))

        v = pxn.decimal("1.1")
        (s, d, e) = v.as_tuple()
        self.assertNotIn(e, ['n', 'N', 'F'])
        self.assertEqual(abs(int(e)), 6)
        self.assertEqual(v, Decimal("1.100000"))


if __name__ == "__main__":
    unittest.main()
