from incline import incline
import logging

log = logging.getLogger('incline')
log.setLevel(logging.INFO)

ramp = incline.InclineClient(
        name='your-datastore-name',
        region='us-east-1',
        rid='123e4567-e89b-12d3-a456-426655440000',
        uid='00000000-0000-0000-0000-000000000000'
        )
try:
    things = ramp.get('0')
except incline.InclineNotFound:
    print 'things not found'
