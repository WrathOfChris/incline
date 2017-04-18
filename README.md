# incline

## including

requirements.txt

```
-e git+git@github.com:WrathOfChris/incline.git@master#egg=incline
```

## using

```
from incline import incline
import logging

log = logging.getLogger('incline')
log.setLevel(logging.INFO)
ids = incline.InclineDatastoreDdb(name='your-datastore-name')
ids.setup()
```
