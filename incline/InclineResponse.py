from dataclasses import dataclass, field
from incline.InclinePrepare import InclinePxn
from incline.InclineRecord import InclineRecord
from incline.error import InclineNotFound, InclineDataError


@dataclass
class InclineResponse:
    """
    pxn: Prepare Transaction ID
    """
    pxn: InclinePxn
    data: dict[str, InclineRecord] = field(default_factory=dict)

    @property
    def only(self) -> InclineRecord:
        """
        Return the first, only, item from a dict of records
        """
        if len(self.data) == 0:
            raise InclineNotFound('only with empty response')
        if len(self.data) != 1:
            raise InclineDataError('only cannot be multiple records')

        # single item list of keys, since keys() is not indexable
        k = list(self.data)[0]
        return self.data[k]
