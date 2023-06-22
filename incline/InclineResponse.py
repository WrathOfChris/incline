from dataclasses import dataclass
from incline.InclinePrepare import InclinePxn


@dataclass
class InclineResponse:
    """
    pxn: Prepare Transaction ID
    """
    pxn: InclinePxn
