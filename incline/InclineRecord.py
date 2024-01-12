from dataclasses import dataclass, field, InitVar
from decimal import Decimal
from typing import Any
from incline.InclineIndex import InclineIndex
from incline.InclineMeta import InclineMeta
from incline.InclinePrepare import InclinePxn


@dataclass
class InclineRecord:
    kid: str = field()
    tsv: Decimal = field(default=Decimal(0), init=False)
    pxn: InclinePxn = field(default_factory=InclinePxn, init=False)
    tmb: Decimal = field(default=Decimal(0),
                         init=False,
                         repr=False,
                         compare=False)
    cid: str = field(default="", init=False, repr=False, compare=False)
    uid: str = field(default="", init=False, repr=False, compare=False)
    rid: str = field(default="", init=False, repr=False, compare=False)
    org: Decimal = field(default=Decimal(0),
                         init=False,
                         repr=False,
                         compare=False)
    ver: int = field(default=0, init=False, repr=False, compare=False)
    met: InclineMeta = field(default_factory=InclineMeta,
                             init=False,
                             repr=False,
                             compare=False)
    dat: Any = field(init=False, repr=False, compare=False)
    idx: dict[str, InclineIndex] = field(default_factory=dict)
    record: InitVar[dict[str, Any] | None] = None

    def __post_init__(self, record: dict[str, Any] | None) -> None:
        if record:
            self.from_dict(record)

    def __format__(self, format_spec: str) -> str:
        return f"kid={self.kid} tsv={self.tsv} pxn={self.pxn}"

    def __str__(self) -> str:
        return f"kid={self.kid} tsv={self.tsv} pxn={self.pxn}"

    @property
    def data(self) -> Any:
        return self.dat

    @property
    def meta(self) -> dict[str, Any]:
        return {
            'kid': self.kid,
            'tsv': self.tsv,
            'pxn': self.pxn,
            'tmb': self.tmb,
            'cid': self.cid,
            'uid': self.uid,
            'rid': self.rid,
            'org': self.org,
            'ver': self.ver,
            'met': self.met
        }

    def from_dict(self, val: dict[str, Any]) -> "InclineRecord":
        if val.get('kid'):
            self.kid = val['kid']

        if val.get('tsv'):
            self.tsv = Decimal(val['tsv'])

        if val.get('pxn'):
            if isinstance(val['pxn'], InclinePxn):
                self.pxn = val['pxn']
            else:
                self.pxn = InclinePxn().loads(val['pxn'])

        if val.get('tmb'):
            self.tmb = Decimal(val['tmb'])

        if val.get('cid'):
            self.cid = val['cid']

        if val.get('uid'):
            self.uid = val['uid']

        if val.get('rid'):
            self.rid = val['rid']

        if val.get('org'):
            self.org = Decimal(val['org'])

        if val.get('ver'):
            self.ver = int(val['ver'])

        if val.get('met'):
            if isinstance(val['met'], InclineMeta):
                self.met = val['met']
            else:
                self.met = InclineMeta().from_dict(val['met'])

        # delete records use dat=None, ensure dat exists
        self.dat = None
        if val.get('dat'):
            self.dat = val['dat']

        for k, v in val.items():
            if k.startswith('idx_'):
                _, _, index_name = k.partition('_')
                self.idx[index_name] = InclineIndex(name=index_name, value=v)

        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            'kid': self.kid,
            'tsv': self.tsv,
            'pxn': self.pxn.pxn,
            'tmb': self.tmb,
            'cid': self.cid,
            'uid': self.uid,
            'rid': self.rid,
            'org': self.org,
            'ver': self.ver,
            'met': self.met.to_dict(),
            'dat': self.dat
        }
