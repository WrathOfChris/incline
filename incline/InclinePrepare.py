from dataclasses import dataclass, field
from decimal import Decimal
import numbers
import time
from typing import SupportsInt
import uuid
from incline.base62 import base_encode, base_decode

INCLINE_TXN_MULTIPLY = 1000000000
INCLINE_TXN_QUANTIZE = "1.000000"
INCLINE_TXN_MONOTIZE = "0.000001"
INCLINE_TXN_CID_JUST = 9
INCLINE_TXN_CNT_JUST = 11
"""
https://docs.python.org/3/library/uuid.html#uuid.getnode

Get the hardware address as a 48-bit positive integer. The first time this
runs, it may launch a separate program, which could be quite slow. If all
attempts to obtain the hardware address fail, we choose a random 48-bit number
with the multicast bit (least significant bit of the first octet) set to 1 as
recommended in RFC 4122. “Hardware address” means the MAC address of a network
interface. On a machine with multiple network interfaces, universally
administered MAC addresses (i.e. where the second least significant bit of the
first octet is unset) will be preferred over locally administered MAC
addresses, but with no other ordering guarantees.
"""
INCLINE_TXN_CLIENTID = uuid.getnode()


@dataclass(order=True)
class InclinePxn:
    """
    Prepare Transaction ID.

    Timestamps should be unique across transactions, and for session
    consistency, increase on a per-client basis. Given unique client IDs, a
    client ID and sequence number form unique transaction timestamps without
    coordination.

    Sort order is (cnt, cid)

    cnt: Timestamp
    cid: ClientID
    pxn: {clientid:9}.{timestamp:11}
    """
    cnt: int = field(default=0)
    cid: int = field(default=INCLINE_TXN_CLIENTID)

    @property
    def pxn(self) -> str:
        cid = base_encode(self.cid).rjust(INCLINE_TXN_CID_JUST, '0')
        cnt = base_encode(self.cnt).rjust(INCLINE_TXN_CNT_JUST, '0')
        return f"{cid}.{cnt}"

    def loads(self, pxn: str) -> "InclinePxn":
        (cid, _, cnt) = pxn.partition('.')
        self.cid = base_decode(cid)
        self.cnt = base_decode(cnt)
        return self

    def __format__(self, format_spec: str) -> str:
        return self.pxn

    def __str__(self) -> str:
        return self.pxn


class InclinePrepare(object):

    def __init__(self, cid: str | SupportsInt | None = None):
        """
        cid: Client ID
        cnt: Counter sequence within Client ID
        tsv: TimeStamp Value
        """
        self.__cid = INCLINE_TXN_CLIENTID
        self.__cidstr = ""
        self.__cnt = int(0)
        self.__tsv = Decimal(0)
        self.__cidstr = self.cid(cid)

    def pxn(self) -> InclinePxn:
        return InclinePxn(cid=self.__cid, cnt=self.cnt())

    def cid(self, cid: str | SupportsInt | None = None) -> str:
        """
        Client ID. Provided by client, MAC address or random number.  Encode
        base-62 if CID is a number.
        """
        if cid:
            if isinstance(cid, SupportsInt):
                self.__cid = int(cid)
            else:
                self.__cid = base_decode(cid)
        if not self.__cidstr:
            self.__cidstr = base_encode(self.__cid).rjust(
                INCLINE_TXN_CID_JUST, '0')
        return self.__cidstr

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
        now = self.decimal(time.time_ns() / INCLINE_TXN_MULTIPLY)

        # Add a microsecond if quantized time is equal or backwards
        if now <= self.__tsv:
            now = self.__tsv + self.decimal(INCLINE_TXN_MONOTIZE)

        self.__tsv = now
        return now

    def decimal(self, number: str | int | float | Decimal) -> Decimal:
        return Decimal(number).quantize(Decimal(INCLINE_TXN_QUANTIZE))
