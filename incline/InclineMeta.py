from dataclasses import dataclass, field

@dataclass
class InclineMetaWrite:
    """
    kid: Key ID
    loc: Location
    pxn: Prepare Transaction ID
    """
    kid: str
    loc: str
    pxn: str = '0'

    def to_dict(self) -> dict[str, str]:
        return {'kid': self.kid, 'loc': self.loc, 'pxn': self.pxn}


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
        Yes, this returns a list instead of a dict. Ugh.
        """
        l: list[dict[str, str]] = []
        for m in self.meta:
            l.append(m.to_dict())
        return l
