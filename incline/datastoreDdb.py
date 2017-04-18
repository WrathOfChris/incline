from datastore import InclineDatastore
from error import InclineError, InclineExists, InclineDataError
import boto3
import decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


"""
LOG FORMAT
{
    kid: key ID
    pxn: prepare ID
    tsv: timestamp
    cid: client ID
    uid: user ID
    rid: request ID
    ver: record version
    met: list of versions
    dat: object
}

TXN FORMAT
{
    kid: key ID
    tsv: timestamp
    pxn: prepare ID from LOG
    tmb: tombstone
    cid: client ID
    uid: user ID
    rid: request ID
    org: origin tsv
    ver: record version
    met: list of versions
    dat: object
}
"""


class InclineDatastoreDdb(InclineDatastore):

    def __init__(self, name='incline', region='us-east-1'):
        self.init(name, region, 'ddb')
        self.ds_init()

    def ds_init(self):
        self.logname = self.name + '-log'
        self.txnname = self.name + '-txn'
        self.logddb = None
        self.logtbl = None
        self.txnddb = None
        self.txntbl = None
        self.logddb = boto3.resource(
            'dynamodb',
            region_name=self.region
        )
        self.txnddb = boto3.resource(
            'dynamodb',
            region_name=self.region
        )
        self.logtbl = self.logddb.Table(self.logname)
        self.txntbl = self.txnddb.Table(self.txnname)

    def ds_get_log(self, kid, pxn=None):
        kwargs = {}
        if pxn:
            self.log.info('getlog %s', kid)
            kwargs['KeyConditionExpression'] = Key(
                'kid').eq(kid) & Key('pxn').eq(pxn)
        else:
            self.log.info('getlog %s pxn %s', kid, pxn)
            kwargs['KeyConditionExpression'] = Key('kid').eq(kid)
            kwargs['ScanIndexForward'] = False
        try:
            resp = self.logtbl.query(**kwargs)
        except ClientError as e:
            raise InclineDataError(e.response['Error']['Message'])
        return self.map_log_response(resp)

    def ds_get_txn(self, kid, tsv=None, limit=1):
        kwargs = {}
        if tsv:
            self.log.info('gettxn %s tsv %s', kid, tsv)
            kwargs['KeyConditionExpression'] = Key(
                'kid').eq(kid) & Key('tsv').eq(tsv)
        else:
            self.log.info('gettxn %s', kid)
            kwargs['KeyConditionExpression'] = Key('kid').eq(kid)
            kwargs['ScanIndexForward'] = False
            if limit:
                kwargs['Limit'] = limit
        try:
            resp = self.txntbl.query(**kwargs)
        except ClientError as e:
            raise InclineDataError(e.response['Error']['Message'])
        return self.map_txn_response(resp)

    def ds_prepare(self, kid, val):
        # XXX validate resp?
        # XXX ReturnValues - ALL_OLD returns prev values
        # XXX ReturnConsumedCapacity
        # XXX ReturnItemCollectionMetrics
        # XXX ConditionExpression - used for atomic create
        resp = self.logtbl.put_item(Item=val)
        # Attributes - returned when ALL_OLD set
        # ConsumedCapacity
        # ItemCollectionMetrics - SizeEstimateRange returned when asked
        return val

    def ds_commit(self, kid, log, create=False):
        # Read current version for origin tsv
        orgtsv = 0
        org = self.only(self.ds_get_txn(kid))
        if org and 'tsv' in org:
            orgtsv = org['tsv']

        # To prevent PutItem from overwriting an existing item, use a
        # conditional expression that specifies that the partition key of the
        # item does not exist. Since every item in the table must have a
        # partition key, this will prevent any existing item from being
        # overwritten
        kwargs = {}
        if create:
            kwargs['ConditionExpression'] = 'attribute_not_exists(kid)'
            # Force unique range key by setting timestamp to 0
            log['tsv'] = 0

        val = self.gentxn(log, tsv=orgtsv)
        try:
            resp = self.txntbl.put_item(Item=val, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise InclineExists('key already exists')
            else:
                raise(e)
        return val

    def map_log_response(self, resp):
        if 'Items' not in resp:
            raise InclineDataError('map log invalid items')
        results = list()
        for r in resp['Items']:
            if 'ver' not in r:
                continue
            if int(r['ver'] == 1):
                r = self.map_log_response_v1(r)
            results.append(r)
        return results

    def map_txn_response(self, resp):
        if 'Items' not in resp:
            raise InclineDataError('map txn invalid items')
        results = list()
        for r in resp['Items']:
            if 'ver' not in r:
                continue
            if int(r['ver'] == 1):
                r = self.map_txn_response_v1(r)
            results.append(r)
        return results

    def map_log_response_v1(self, resp):
        keys = ['kid', 'pxn', 'tsv', 'cid', 'uid', 'rid', 'ver', 'met', 'dat']
        r = dict()
        for k in keys:
            if k in resp:
                if k == 'ver':
                    r[k] = int(resp[k])
                else:
                    r[k] = resp[k]
        return r

    def map_txn_response_v1(self, resp):
        keys = [
            'kid',
            'tsv',
            'pxn',
            'tmb',
            'cid',
            'uid',
            'rid',
            'org',
            'ver',
            'met',
            'dat']
        r = dict()
        for k in keys:
            if k in resp:
                if k == 'ver':
                    r[k] = int(resp[k])
                elif k == 'tsv':
                    r[k] = decimal.Decimal(resp[k])
                elif k == 'tmb':
                    r[k] = bool(resp[k])
                else:
                    r[k] = resp[k]
        return r

    def ds_setup(self):
        self.ds_setup_log()
        self.ds_setup_txn()

    def ds_setup_log(self, rcu=1, wcu=1):
        tablename = self.name + '-log'
        response = self.logddb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'kid',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'pxn',
                    'AttributeType': 'S'
                },
            ],
            TableName=tablename,
            KeySchema=[
                {
                    'AttributeName': 'kid',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'pxn',
                    'KeyType': 'RANGE'
                },
            ],
            # LocalSecondaryIndexes=[
            #    {
            #        'IndexName': tablename + '-idx',
            #        'KeySchema': [
            #            {
            #                'AttributeName': 'kid',
            #                'KeyType': 'HASH'
            #            },
            #            {
            #                'AttributeName': 'pxn',
            #                'KeyType': 'RANGE'
            #            },
            #        ],
            #        'Projection': {
            #            'ProjectionType': 'INCLUDE',
            #            'NonKeyAttributes': [
            #                'tsv',
            #                'cid',
            #                'uid',
            #                'rid',
            #                'ver',
            #                'met'
            #            ]
            #        }
            #    },
            #],
            ProvisionedThroughput={
                'ReadCapacityUnits': rcu,
                'WriteCapacityUnits': wcu
            }
        )

    def ds_setup_txn(self, rcu=1, wcu=1):
        tablename = self.name + '-txn'
        response = self.txnddb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'kid',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'tsv',
                    'AttributeType': 'N'
                },
            ],
            TableName=tablename,
            KeySchema=[
                {
                    'AttributeName': 'kid',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'tsv',
                    'KeyType': 'RANGE'
                },
            ],
            # LocalSecondaryIndexes=[
            #    {
            #        'IndexName': tablename + '-idx',
            #        'KeySchema': [
            #            {
            #                'AttributeName': 'kid',
            #                'KeyType': 'HASH'
            #            },
            #            {
            #                'AttributeName': 'tsv',
            #                'KeyType': 'RANGE'
            #            },
            #        ],
            #        'Projection': {
            #            'ProjectionType': 'INCLUDE',
            #            'NonKeyAttributes': [
            #                'pxn',
            #                'tmb',
            #                'cid',
            #                'uid',
            #                'rid',
            #                'ver',
            #                'met'
            #            ]
            #        }
            #    },
            #],
            ProvisionedThroughput={
                'ReadCapacityUnits': rcu,
                'WriteCapacityUnits': wcu
            }
        )
