from dataclasses import dataclass, field, InitVar
from typing import Any
from incline.InclinePrepare import InclinePxn


@dataclass
class InclineMetaWrite:
    """
    kid: Key ID
    loc: Location
    pxn: Prepare Transaction ID
    """
    kid: str = field(default="")
    loc: str = field(default="")
    pxn: InclinePxn = field(default_factory=InclinePxn)
    meta: InitVar[dict[str, Any] | None] = None

    def __post_init__(self, meta: dict[str, Any] | None) -> None:
        if meta:
            self.from_dict(meta)

    def to_dict(self) -> dict[str, str]:
        return {'kid': self.kid, 'loc': self.loc, 'pxn': self.pxn.pxn}

    def from_dict(self, val: dict[str, Any]) -> "InclineMetaWrite":
        if val.get('kid'):
            self.kid = val['kid']

        if val.get('loc'):
            self.loc = val['loc']

        if val.get('pxn'):
            self.pxn = InclinePxn().loads(val['pxn'])

        return self


@dataclass
class InclineMeta:
    """
    meta: WriteSet list of InclineMetaWrite
    """
    meta: list[InclineMetaWrite] = field(default_factory=list)

    def add_write(self, write: InclineMetaWrite) -> None:
        self.meta.append(write)

    def from_dict(self,
                  val: list[dict[str, str]] | dict[str, str]) -> "InclineMeta":
        """
        Ok, again, this really comes from a list
        """
        if not isinstance(val, list):
            val = [val]
        for v in val:
            self.add_write(InclineMetaWrite(meta=v))

        return self

    def to_dict(self) -> list[dict[str, str]]:
        """
        Ok, this really returns a list
        """
        out: list[dict[str, str]] = []
        for m in self.meta:
            out.append(m.to_dict())
        return out
