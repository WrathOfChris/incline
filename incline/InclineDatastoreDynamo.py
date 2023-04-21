from incline.InclineDatastore import InclineDatastore
from incline.error import (InclineError, InclineExists, InclineDataError,
                           InclineNotFound)
import boto3
import decimal
import copy
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# OpenTelemetry Instrumenting
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
import botocore

# Instrument Botocore
BotocoreInstrumentor().instrument()

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


class InclineDatastoreDynamo(InclineDatastore):

    def __init__(self, name='incline', region='us-west-2'):
        self.init(name, region, dbtype='dynamo')
        self.ds_init()

    def ds_init(self):
        self.logname = self.name + '-log'
        self.txnname = self.name + '-txn'
        self.dynamores = None
        self.dynamoclient = None
        self.logtbl = None
        self.txntbl = None
        with self.trace.span("aws.dynamodb.resource") as span:
            self.dynamores = boto3.resource('dynamodb',
                                            region_name=self.region)
        with self.trace.span("aws.dynamodb.table") as span:
            span.set_attribute("dynamo.table", self.logname)
            self.logtbl = self.dynamores.Table(self.logname)
        with self.trace.span("aws.dynamodb.table") as span:
            span.set_attribute("dynamo.table", self.txnname)
            self.txntbl = self.dynamores.Table(self.txnname)
        with self.trace.span("aws.dynamodb.client") as span:
            self.dynamoclient = boto3.client('dynamodb',
                                             region_name=self.region)

    def ds_get_log(self, kid, pxn=None):
        request_args = locals()
        if not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")

        with self.trace.span("incline.datastore.ds_get_log") as span:
            self.map_request_span(request_args, span)

            kwargs = {}
            if pxn:
                self.log.info('getlog %s pxn %s', kid, pxn)
                kwargs['KeyConditionExpression'] = Key('kid').eq(kid) & Key(
                    'pxn').eq(pxn)
            else:
                self.log.info('getlog %s', kid)
                kwargs['KeyConditionExpression'] = Key('kid').eq(kid)
                kwargs['ScanIndexForward'] = False

            with self.trace.span("aws.dynamodb.query") as span:
                try:
                    resp = self.logtbl.query(**kwargs)
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])
                self.map_aws_response_span(resp, span)

            # XXX validate resp?  Count.  Items.

        return self.map_log_response(resp)

    def ds_get_txn(self, kid, tsv=None, limit=1):
        """
        get from committed transaction table

        reverse query to scan from new to old with ScanIndexForward

        can change behaviour from exact to "active at the time" using lte(tsv)
        """
        request_args = locals()
        with self.trace.span("incline.datastore.ds_get_txn") as span:
            self.map_request_span(request_args, span)

            if not isinstance(kid, str):
                raise InclineInterface(f"key must be string not {type(kid)}")

            if tsv and not isinstance(tsv, decimal.Decimal):
                tsv = self.pxn.decimal(tsv)

            kwargs = {}
            if tsv:
                self.log.info('gettxn %s tsv %s', kid, tsv)
                kwargs['KeyConditionExpression'] = Key('kid').eq(kid) & Key(
                    'tsv').eq(tsv)
            else:
                self.log.info('gettxn %s', kid)
                kwargs['KeyConditionExpression'] = Key('kid').eq(kid)
                kwargs['ScanIndexForward'] = False
                if limit:
                    kwargs['Limit'] = limit

            with self.trace.span("aws.dynamodb.query") as span:
                try:
                    resp = self.txntbl.query(**kwargs)
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])
                self.map_aws_response_span(resp, span)

            # XXX validate resp?  Count.  Items.

        return self.map_txn_response(resp)

    def ds_prepare(self, kid, val):
        request_args = locals()
        # XXX validate resp?
        # XXX ReturnValues - ALL_OLD returns prev values
        # XXX ReturnConsumedCapacity
        # XXX ReturnItemCollectionMetrics
        # XXX ConditionExpression - used for atomic create

        with self.trace.span("incline.datastore.ds_prepare") as span:
            self.map_request_span(request_args, span)

            # convert numbers to remote representation
            # DynamoDB uses Decimal, does not support float
            remote_val = self.numbers_to_remote(copy.deepcopy(val))

            with self.trace.span("aws.dynamodb.put_item") as span:
                resp = self.logtbl.put_item(Item=remote_val)

                # Attributes - returned when ALL_OLD set
                # ConsumedCapacity
                # ItemCollectionMetrics - SizeEstimateRange returned when asked

                self.map_aws_response_span(resp, span)
        return [val]

    def ds_commit(self, kid, log, create=False):
        request_args = locals()
        with self.trace.span("incline.datastore.ds_commit") as span:
            self.map_request_span(request_args, span)

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
            with self.trace.span("aws.dynamodb.put_item") as span:
                try:
                    resp = self.txntbl.put_item(Item=val, **kwargs)
                except ClientError as e:
                    if e.response['Error'][
                            'Code'] == 'ConditionalCheckFailedException':
                        raise InclineExists('key already exists')
                    else:
                        raise (e)
                self.map_aws_response_span(resp, span)

        return [val]

    def ds_scan_log(self, kid=None, tsv=None, limit=None):
        """
        return list of [{'kid': kid, 'tsv': tsv}]
        """
        request_args = locals()
        logs = list()

        if kid and not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")

        with self.trace.span("incline.datastore.ds_scan_log") as span:
            self.map_request_span(request_args, span)

            kwargs = {}
            if kid and tsv:
                self.log.info(f"scanlog {kid} tsv {tsv}")
                kwargs['FilterExpression'] = \
                        Key('key').eq(kid) & Key('tsv').le(tsv)
            elif kid and not tsv:
                self.log.info(f"scanlog {kid}")
                kwargs['FilterExpression'] = Key('key').eq(kid)
            elif not kid and tsv:
                self.log.info(f"scanlog tsv {tsv}")
                kwargs['FilterExpression'] = \
                        Key('key').eq(kid) & Key('tsv').le(tsv)
            else:
                self.log.info(f"scanlog (all)")

            with self.trace.span("aws.dynamodb.scan") as span:
                paginator = self.dynamoclient.get_paginator('scan')
                resp = paginator.paginate(
                        TableName=self.logname,
                        Select='SPECIFIC_ATTRIBUTES',
                        ProjectionExpression='kid, pxn, ver',
                        ConsistentRead=False,
                        **kwargs)
                try:
                    for page in resp:
                        self.map_aws_response_span(page, span)

                        # empty page, likely FilterExpression filtered all
                        if page.get('Count') == 0 or not len(page['Items']):
                            continue

                        items = self.map_scan_log_response(page)
                        for item in items:
                            logs.append({'kid': item['kid'],
                                         'pxn': item['pxn']})
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])

                # XXX validate resp?  Count.  Items.
        return logs

    def ds_scan_txn(self, kid=None, tsv=None, limit=None):
        """
        return list of [{'kid': kid, 'tsv': tsv}]
        """
        request_args = locals()
        txns = list()

        if kid and not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")

        with self.trace.span("incline.datastore.ds_scan_txn") as span:
            self.map_request_span(request_args, span)

            kwargs = {}
            if kid and tsv:
                self.log.info(f"scantxn {kid} tsv {tsv}")
                kwargs['FilterExpression'] = \
                        Key('key').eq(kid) & Key('tsv').le(tsv)
            elif kid and not tsv:
                self.log.info(f"scantxn {kid}")
                kwargs['FilterExpression'] = Key('key').eq(kid)
            elif not kid and tsv:
                self.log.info(f"scantxn tsv {tsv}")
                kwargs['FilterExpression'] = \
                        Key('key').eq(kid) & Key('tsv').le(tsv)
            else:
                self.log.info(f"scantxn (all)")

            with self.trace.span("aws.dynamodb.scan") as span:
                paginator = self.dynamoclient.get_paginator('scan')
                resp = paginator.paginate(
                        TableName=self.txnname,
                        Select='SPECIFIC_ATTRIBUTES',
                        ProjectionExpression='kid, tsv, ver',
                        ConsistentRead=False,
                        **kwargs)
                try:
                    for page in resp:
                        self.map_aws_response_span(resp, span)

                        # empty page, likely FilterExpression filtered all
                        if page.get('Count') == 0 or not len(page['Items']):
                            continue

                        items = self.map_scan_txn_response(page)
                        for item in items:
                            txns.append({'kid': item['kid'],
                                         'tsv': item['tsv']})
                        if 'Items' not  in page:
                            raise InclineError(f"bad api response {page}")
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])

                # XXX validate resp?  Count.  Items.
        return txns

    def ds_delete_log(self, kid, pxn):
        request_args = locals()
        if not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")

        with self.trace.span("incline.datastore.ds_delete_log") as span:
            self.map_request_span(request_args, span)

            with self.trace.span("aws.dynamo.delete_item"):
                try:
                    resp = self.logtbl.delete_item(
                            Key={'kid': kid, 'pxn': pxn},
                            ReturnValues='ALL_OLD')
                except ClientError as e:
                    raise (e)

                if ('Attributes' not in resp
                    or resp['Attributes'].get('kid') != kid
                    or resp['Attributes'].get('pxn') != pxn):
                    raise InclineNotFound(f"cannot delete {kid} pxn {pxn}")

    def ds_delete_txn(self, kid, tsv):
        request_args = locals()
        if not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")
        if not isinstance(tsv, decimal.Decimal):
            tsv = self.pxn.decimal(tsv)

        with self.trace.span("incline.datastore.ds_delete_txn") as span:
            self.map_request_span(request_args, span)

            with self.trace.span("aws.dynamo.delete_item"):
                try:
                    resp = self.txntbl.delete_item(
                            Key={'kid': kid, 'tsv': tsv},
                            ReturnValues='ALL_OLD')
                except ClientError as e:
                    raise (e)

                if ('Attributes' not in resp
                    or resp['Attributes'].get('kid') != kid
                    or resp['Attributes'].get('tsv') != tsv):
                    raise InclineNotFound(f"cannot delete {kid} tsv {tsv}")

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

    def map_scan_log_response(self, resp):
        if 'Items' not in resp:
            raise InclineDataError('map scan invalid items')

        """
        Pagination comes from DynamoDB.Client which is a low-level client.
        Use the internal boto3 deserializer that Table() uses
        """
        deserializer = boto3.dynamodb.types.TypeDeserializer()

        results = list()
        for r in resp['Items']:
            data = {k: deserializer.deserialize(v) for k, v in r.items()}
            if 'ver' not in data:
                continue
            if int(data['ver'] == 1):
                data = self.map_log_response_v1(data)
            results.append(data)
        return results

    def map_scan_txn_response(self, resp):
        if 'Items' not in resp:
            raise InclineDataError('map scan invalid items')

        """
        Pagination comes from DynamoDB.Client which is a low-level client.
        Use the internal boto3 deserializer that Table() uses
        """
        deserializer = boto3.dynamodb.types.TypeDeserializer()

        results = list()
        for r in resp['Items']:
            data = {k: deserializer.deserialize(v) for k, v in r.items()}
            if 'ver' not in data:
                continue
            if int(data['ver'] == 1):
                data = self.map_txn_response_v1(data)
            results.append(data)
        return results

    def map_aws_response_span(self, resp, span):
        if not resp or not isinstance(resp, dict):
            return

        if 'Count' in resp:
            span.set_attribute("response.count", resp['Count'])
        if 'ScannedCount' in resp:
            span.set_attribute("response.scanned", resp['ScannedCount'])
        if 'ResponseMetadata' in resp:
            meta = resp['ResponseMetadata']
            if 'RetryAttempts' in meta:
                span.set_attribute("response.retries", meta['RetryAttempts'])
            if 'httpStatusCode' in meta:
                span.set_attribute("response.status", meta['httpStatusCode'])
            if 'RequestId' in meta:
                span.set_attribute("response.rid", meta['RequestId'])


    def ds_setup(self):
        self.ds_setup_log()
        self.ds_setup_txn()

    def ds_setup_log(self, rcu=1, wcu=1):
        tablename = self.name + '-log'
        response = self.dynamores.create_table(
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
            })

    def ds_setup_txn(self, rcu=1, wcu=1):
        tablename = self.name + '-txn'
        response = self.dynamores.create_table(
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
            })
