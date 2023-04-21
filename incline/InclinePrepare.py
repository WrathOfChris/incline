import decimal
import numbers
import time
import uuid
from incline.base62 import base_encode
from incline import (INCLINE_TXN_MULTIPLY, INCLINE_TXN_QUANTIZE,
                     INCLINE_TXN_BASEJUST)


class InclinePrepare(object):
    def __init__(self, cid=None):
        """
        cid: Client ID
        tsv: TimeStamp Value
        """
        self.__cid = cid
        self.__tsv = decimal.Decimal(0)
        self.__cnt = self.__tsv

    def pxn(self):
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

    def cmppxn(self, pxn1, pxn2):
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

    def cid(self, cid=None):
        """
        Client ID. Provided by client, MAC address or random number.  Encode
        base-62 if CID is a number.
        """
        if not self.__cid:
            if not cid:
                cid = uuid.getnode()
            if isinstance(cid, numbers.Number):
                self.__cid = base_encode(cid)
            else:
                self.__cid = cid
        return self.__cid

    def cnt(self, cnt=None):
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

    def now(self):
        """
        Monotonic nanosecond UTC timestamp
        """
        now = self.decimal(time.time_ns()) / INCLINE_TXN_MULTIPLY
        if now <= self.__tsv:
            now = self.__tsv + self.decimal('0.000001')
        self.__tsv = now
        return now

    def decimal(self, number):
        return decimal.Decimal(number).quantize(
            decimal.Decimal(INCLINE_TXN_QUANTIZE))
