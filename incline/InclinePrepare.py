from decimal import Decimal
import numbers
import time
import uuid
from incline.base62 import base_encode

INCLINE_TXN_MULTIPLY = 1000000000
INCLINE_TXN_QUANTIZE = "1.000000"
INCLINE_TXN_BASEJUST = 11


class InclinePrepare(object):

    def __init__(self, cid: str | None = None):
        """
        cid: Client ID
        tsv: TimeStamp Value
        """
        self.__cid = cid
        self.__tsv = Decimal(0)
        self.__cnt = int(0)

    def pxn(self) -> str:
        """
        Prepare Transaction ID.
        Timestamps should be unique across transactions, and for session
        consistency, increase on a per-client basis. Given unique client IDs, a
        client ID and sequence number form unique transaction timestamps
        without coordination.  Use base62 encoding, padded to 11 digits
        """
        pxn = '{0}.{1}'
        return pxn.format(
            self.cid(),
            base_encode(self.cnt()).rjust(INCLINE_TXN_BASEJUST, '0'))

    def cmppxn(self, pxn1: str, pxn2: str) -> int:
        cid1 = pxn1.split('.')[0]
        cnt1 = pxn1.split('.')[1]
        cid2 = pxn2.split('.')[0]
        cnt2 = pxn2.split('.')[1]

        # Compare counter
        if cnt1 < cnt2:
            return -1
        elif cnt1 > cnt2:
            return 1

        # Compare clientID
        if cid1 < cid2:
            return -1
        elif cid1 > cid2:
            return 1

        # Same ClientID and Counter
        return 0

    def cid(self, cid: str | int | None = None) -> str:
        """
        Client ID. Provided by client, MAC address or random number.  Encode
        base-62 if CID is a number.
        """
        if not self.__cid:
            if not cid:
                cid = uuid.getnode()
            if isinstance(cid, numbers.Number):
                self.__cid = base_encode(int(cid))
            else:
                self.__cid = str(cid)
        return self.__cid

    def cnt(self, cnt: int | None = None) -> int:
        """
        Monotonic timestamp counter
        """
        now = int(self.now() * INCLINE_TXN_MULTIPLY)
        if not cnt or cnt < now:
            cnt = now
        if cnt < self.__cnt:
            cnt = self.__cnt + 1
        self.__cnt = cnt
        return self.__cnt

    def now(self) -> Decimal:
        """
        Monotonic nanosecond UTC timestamp
        """
        now = self.decimal(time.time_ns()) / INCLINE_TXN_MULTIPLY
        if now <= self.__tsv:
            now = self.__tsv + self.decimal('0.000001')
        self.__tsv = now
        return now

    def decimal(self, number: str | int | float | Decimal) -> Decimal:
        return Decimal(number).quantize(Decimal(INCLINE_TXN_QUANTIZE))
