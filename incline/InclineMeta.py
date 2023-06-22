from dataclasses import asdict, dataclass, field
from incline.InclinePrepare import InclinePxn


@dataclass
class InclineMetaWrite:
    """
    kid: Key ID
    loc: Location
    pxn: Prepare Transaction ID
    """
    kid: str
    loc: str
    pxn: InclinePxn = field(default_factory=InclinePxn)

    def to_dict(self) -> dict[str, str]:
        return {'kid': self.kid, 'loc': self.loc, 'pxn': self.pxn.pxn}


@dataclass
class InclineMeta:
    """
    meta: WriteSet list of InclineMetaWrite
    """
    meta: list[InclineMetaWrite] = field(default_factory=list)

    def add_write(self, write: InclineMetaWrite) -> None:
        self.meta.append(write)

    def to_dict(self) -> list[dict[str, str]]:
        """
        Ok, this really returns a list
        """
        out: list[dict[str, str]] = []
        for m in self.meta:
            out.append(m.to_dict())
        return out
