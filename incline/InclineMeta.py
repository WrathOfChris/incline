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
        return asdict(self)


@dataclass
class InclineMeta:
    """
    meta: WriteSet list of InclineMetaWrite
    """
    meta: list[InclineMetaWrite] = field(default_factory=list)

    def add_write(self, write: InclineMetaWrite) -> None:
        self.meta.append(write)

    def to_dict(self) -> dict[str, str]:
        """
        Ok, this really returns a list
        """
        return list(asdict(self)['meta'])
