from incline.InclineDatastore import InclineDatastore
from incline.InclinePrepare import InclinePxn
from incline.InclineTrace import InclineTrace
from incline.error import (InclineError, InclineExists, InclineDataError,
                           InclineNotFound, InclineInterface)
import boto3
import copy
from decimal import Decimal
from typing import Any
import botocore.config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.types import TypeDeserializer
from opentelemetry.trace.span import Span

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

    def __init__(self,
                 name: str = 'incline',
                 region: str = 'us-west-2',
                 trace: InclineTrace | None = None):
        self.init(name, region, dbtype='dynamo', trace=trace)
        self.ds_init()

    def ds_init(self) -> None:
        self.logname = self.name + '-log'
        self.txnname = self.name + '-txn'
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
            self.dynamoclient = boto3.client(
                'dynamodb',
                region_name=self.region,
                config=botocore.config.Config(retries={'mode': 'adaptive'}))

    def ds_get_log(self,
                   kid: str,
                   pxn: InclinePxn | None = None) -> list[dict[str, Any]]:
        request_args = locals()
        if not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")

        with self.trace.span("incline.datastore.ds_get_log") as span:
            self.map_request_span(request_args, span)

            kwargs = {}
            if pxn:
                self.log.info('getlog %s pxn %s', kid, format(pxn))
                kwargs['KeyConditionExpression'] = Key('kid').eq(kid) & Key(
                    'pxn').eq(pxn.pxn)
            else:
                self.log.info('getlog %s', kid)
                kwargs['KeyConditionExpression'] = Key('kid').eq(
                        kid)   # type: ignore
                kwargs['ScanIndexForward'] = False    # type:ignore

            with self.trace.span("aws.dynamodb.query") as span_query:
                try:
                    resp = self.logtbl.query(**kwargs)
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])
                self.map_aws_response_span(resp, span_query)

            # XXX validate resp?  Count.  Items.

            local_resp = self.map_log_response_dynamo(resp)
            self.map_response_span(local_resp, span)
            return local_resp

    def ds_get_txn(self,
                   kid: str,
                   tsv: int | str | Decimal | None = None,
                   limit: int = 1) -> list[dict[str, Any]]:
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

            if tsv and not isinstance(tsv, Decimal):
                tsv = self.pxn.decimal(tsv)

            kwargs = {}
            if tsv:
                self.log.info('gettxn %s tsv %s', kid, tsv)
                kwargs['KeyConditionExpression'] = Key('kid').eq(kid) & \
                        Key('tsv').lte(tsv)
            else:
                self.log.info('gettxn %s', kid)
                kwargs['KeyConditionExpression'] = Key('kid').eq(
                        kid)    # type: ignore
                kwargs['ScanIndexForward'] = False    # type: ignore
                if limit:
                    kwargs['Limit'] = limit    # type: ignore

            with self.trace.span("aws.dynamodb.query") as span_query:
                try:
                    resp = self.txntbl.query(**kwargs)
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])
                self.map_aws_response_span(resp, span_query)

            # XXX validate resp?  Count.  Items.

            local_resp = self.map_txn_response_dynamo(resp)
            self.map_response_span(local_resp, span)
            return local_resp

    def ds_prepare(self, kid: str, val: dict[str,
                                             Any]) -> list[dict[str, Any]]:
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

            with self.trace.span("aws.dynamodb.put_item") as span_put:
                resp = self.logtbl.put_item(Item=remote_val,
                                            ReturnValues='ALL_OLD')

                # Attributes - returned when ALL_OLD set
                # ConsumedCapacity
                # ItemCollectionMetrics - SizeEstimateRange returned when asked

                self.map_aws_response_span(resp, span_put)
                return [val]

    def ds_commit(self,
                  kid: str,
                  log: Any,
                  mode: str | None = None) -> list[dict[str, Any]]:
        request_args = locals()
        with self.trace.span("incline.datastore.ds_commit") as span:
            self.map_request_span(request_args, span)

            # Read current version for origin tsv
            # NOTE: Tombstone race between origin.tsv -> create.tsv starts here
            orgtsv = 0
            org = self.only(self.ds_get_txn(kid))
            if org and 'tsv' in org:
                if mode == "refresh":
                    # Refresh persists the origin timestamp
                    orgtsv = org['org']
                else:
                    # Set origin to latest transaction timestamp
                    orgtsv = org['tsv']
            if org:
                self.map_txn_span(org, span, prefix="org")

            kwargs = {}

            if mode == 'create':
                # To prevent PutItem from overwriting an existing item, use a
                # conditional expression that specifies that the partition key
                # of the item does not exist. Since every item in the table
                # must have a partition key, this will prevent any existing
                # item from being overwritten
                if not org:
                    kwargs['ConditionExpression'] = Attr('kid').not_exists()
                else:
                    # Tombstones prevent ConditionExpression on the partition
                    # key from working as a condition.
                    #
                    # TODO consider transact_write_items() to mitigate race
                    #      between origin.tsv --> create.tsv

                    # Check origin when create was prepared.  If not a
                    # tombstone, then key existed at prepare
                    if not self.is_txn_deleted(org, log.get('tsv')):
                        raise InclineExists('key already exists')

                    # Check again, noting the create was after prepare
                    if not self.is_txn_deleted(org):
                        raise InclineExists('key exists, create after prepare')

            # convert numbers to remote representation
            # DynamoDB uses Decimal, does not support float
            remote_log = self.numbers_to_remote(copy.deepcopy(log))

            if mode == 'delete':
                remote_log['dat'] = None

            val = self.gentxn(remote_log, tsv=orgtsv)
            self.map_txn_span(val, span, prefix="txn")
            with self.trace.span("aws.dynamodb.put_item") as span_put:
                try:
                    resp = self.txntbl.put_item(Item=val,
                                                ReturnValues='ALL_OLD',
                                                **kwargs)
                except ClientError as e:
                    if e.response['Error'][
                            'Code'] == 'ConditionalCheckFailedException':
                        raise InclineExists('key already exists')
                    else:
                        raise (e)
                self.map_aws_response_span(resp, span_put)
                # TODO: ALL_OLD
                return [val]

    def ds_scan_log(self,
                    kid: str | None = None,
                    tsv: Decimal | None = None,
                    limit: int | None = None) -> list[dict[str, Any]]:
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
                        Key('kid').eq(kid) & Key('tsv').lte(tsv)
            elif kid and not tsv:
                self.log.info(f"scanlog {kid}")
                kwargs['FilterExpression'] = Key('kid').eq(
                        kid)    # type: ignore
            elif not kid and tsv:
                self.log.info(f"scanlog tsv {tsv}")
                kwargs['FilterExpression'] = \
                        Key('kid').eq(kid) & Key('tsv').lte(tsv)
            else:
                self.log.info(f"scanlog (all)")

            with self.trace.span("aws.dynamodb.scan") as span_scan:
                paginator = self.dynamoclient.get_paginator('scan')
                resp = paginator.paginate(TableName=self.logname,
                                          Select='SPECIFIC_ATTRIBUTES',
                                          ProjectionExpression='kid, pxn, ver',
                                          ConsistentRead=False,
                                          **kwargs)
                try:
                    for page in resp:
                        self.map_aws_response_span(page, span_scan)

                        # empty page, likely FilterExpression filtered all
                        if page.get('Count') == 0 or not len(page['Items']):
                            continue

                        items = self.map_scan_log_response(page)
                        for item in items:
                            logs.append({
                                'kid': item['kid'],
                                'pxn': item['pxn']
                            })
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])

                # XXX validate resp?  Count.  Items.
                return logs

    def ds_scan_txn(self,
                    kid: str | None = None,
                    tsv: Decimal | int | str | None = None,
                    limit: int | None = None) -> list[dict[str, Any]]:
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
                        Key('kid').eq(kid) & Key('tsv').lte(tsv)
            elif kid and not tsv:
                self.log.info(f"scantxn {kid}")
                kwargs['FilterExpression'] = Key('kid').eq(
                        kid)    # type: ignore
            elif not kid and tsv:
                self.log.info(f"scantxn tsv {tsv}")
                kwargs['FilterExpression'] = \
                        Key('kid').eq(kid) & Key('tsv').lte(tsv)
            else:
                self.log.info(f"scantxn (all)")

            with self.trace.span("aws.dynamodb.scan") as span_scan:
                paginator = self.dynamores.meta.client.get_paginator('scan')
                # paginator = self.dynamoclient.get_paginator('scan')
                #
                # https://github.com/boto/boto3/issues/2300
                # Invalid type for parameter FilterExpression,
                #   value: <boto3.dynamodb.conditions.Equals ...
                #   t-ype: <class 'boto3.dynamodb.conditions.Equals'>
                #   valid types: <class 'str'>
                resp = paginator.paginate(TableName=self.txnname,
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
                            txns.append({
                                'kid': item['kid'],
                                'tsv': item['tsv']
                            })
                        if 'Items' not in page:
                            raise InclineError(f"bad api response {page}")
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])

                # XXX validate resp?  Count.  Items.
                return txns

    def ds_delete_log(self, kid: str, pxn: InclinePxn) -> None:
        request_args = locals()
        if not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")

        with self.trace.span("incline.datastore.ds_delete_log") as span:
            self.map_request_span(request_args, span)

            with self.trace.span("aws.dynamo.delete_item") as span_delete:
                try:
                    resp = self.logtbl.delete_item(Key={
                        'kid': kid,
                        'pxn': pxn.pxn
                    },
                                                   ReturnValues='ALL_OLD')
                except ClientError as e:
                    raise (e)

                if ('Attributes' not in resp
                        or resp['Attributes'].get('kid') != kid
                        or resp['Attributes'].get('pxn') != pxn.pxn):
                    raise InclineNotFound(f"cannot delete {kid} " \
                            f"pxn {format(pxn)}")

    def ds_delete_txn(self, kid: str, tsv: Decimal | int | str) -> None:
        request_args = locals()
        if not isinstance(kid, str):
            raise InclineInterface(f"key must be string not {type(kid)}")
        if not isinstance(tsv, Decimal):
            tsv = self.pxn.decimal(tsv)

        with self.trace.span("incline.datastore.ds_delete_txn") as span:
            self.map_request_span(request_args, span)

            with self.trace.span("aws.dynamo.delete_item") as span_delete:
                try:
                    resp = self.txntbl.delete_item(Key={
                        'kid': kid,
                        'tsv': tsv
                    },
                                                   ReturnValues='ALL_OLD')
                except ClientError as e:
                    raise (e)

                if ('Attributes' not in resp
                        or resp['Attributes'].get('kid') != kid
                        or resp['Attributes'].get('tsv') != tsv):
                    raise InclineNotFound(f"cannot delete {kid} tsv {tsv}")

    def ds_get_idx(self,
                   idx: str,
                   val: Any) -> list[dict[str, Any]]:
        """
        get from index
        """
        request_args = locals()
        with self.trace.span("incline.datastore.ds_get_idx") as span:
            self.map_request_span(request_args, span)

            if not isinstance(idx, str):
                raise InclineInterface(f"idx must be string not {type(idx)}")

            kwargs: dict[str, Any] = {}
            self.log.info('getidx %s val %s', idx, val)
            kwargs['KeyConditionExpression'] = Key(f"idx_{idx}").eq(val)
            kwargs['IndexName'] = f"{self.txnname}-idx-{idx}"

            with self.trace.span("aws.dynamodb.query") as span_query:
                try:
                    resp = self.txntbl.query(**kwargs)
                except ClientError as e:
                    raise InclineDataError(e.response['Error']['Message'])
                self.map_aws_response_span(resp, span_query)

            # XXX validate resp?  Count.  Items.

            local_resp = self.map_idx_response_dynamo(idx, resp)
            self.map_response_span(local_resp, span)
            return local_resp

    def map_log_response_dynamo(self, resp: dict[str,
                                                 Any]) -> list[dict[str, Any]]:
        """
        Pass Dynamo Items to InclineDatastore.map_log_response
        """
        if 'Items' not in resp:
            raise InclineDataError('map log invalid items')
        return self.map_log_response(resp['Items'])

    def map_txn_response_dynamo(self, resp: dict[str,
                                                 Any]) -> list[dict[str, Any]]:
        if 'Items' not in resp:
            raise InclineDataError('map txn invalid items')
        return self.map_txn_response(resp['Items'])

    def map_idx_response_dynamo(self,
                                idx: str,
                                resp: dict[str, Any]) -> list[dict[str, Any]]:
        if 'Items' not in resp:
            raise InclineDataError('map idx invalid items')
        items = resp['Items']
        if not isinstance(items, list):
            items = [items]

        r = []
        for item in items:
            # {'idx_tid': 'T0ZQN0VmTcnwPipn391Vrtr',
            #  'tsv': Decimal('1701825790.360248'),
            #  'pxn': '0ryIfPzwQ.21iNZOIOsUS',
            #  'kid': 'C08XeIPmJXgOzKnskv3D93S'}
            data = {}
            for k, v in item.items():
                if k.startswith('idx_'):
                    # drop the searched value, it is known
                    continue
                else:
                    data[k] = v
            r.append(data)

        return r

    def map_scan_log_response(self, resp: dict[str,
                                               Any]) -> list[dict[str, Any]]:
        if 'Items' not in resp:
            raise InclineDataError('map scan invalid items')
        """
        Pagination comes from DynamoDB.Client which is a low-level client.
        Use the internal boto3 deserializer that Table() uses
        """
        deserializer = TypeDeserializer()

        results = list()
        for r in resp['Items']:
            data = {k: deserializer.deserialize(v) for k, v in r.items()}
            if 'ver' not in data:
                continue
            if int(data['ver'] == 1):
                data = self.map_log_response_v1(data)
            results.append(data)
        return results

    def map_scan_txn_response(self, resp: dict[str,
                                               Any]) -> list[dict[str, Any]]:
        if 'Items' not in resp:
            raise InclineDataError('map scan invalid items')
        """
        Pagination comes from DynamoDB.Client which is a low-level client.
        Use the internal boto3 deserializer that Table() uses
        """
        deserializer = TypeDeserializer()

        results = list()
        for r in resp['Items']:
            # TODO check if deserialize still necessary
            #data = {k: deserializer.deserialize(v) for k, v in r.items()}
            data = r
            if 'ver' not in data:
                continue
            if int(data['ver'] == 1):
                data = self.map_txn_response_v1(data)
            results.append(data)
        return results

    def map_aws_response_span(self, resp: dict[str, Any], span: Span) -> None:
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

    def ds_setup(self) -> None:
        self.ds_setup_log()
        self.ds_setup_txn()

    def ds_setup_log(self, rcu: int = 1, wcu: int = 1) -> None:
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
        # TODO: waiter = client.get_waiter('table_exists')
        # TODO: waiter.wait(TableName=..., WaiterConfig={'Delay':  1})

    def ds_setup_txn(self, rcu: int = 1, wcu: int = 1) -> None:
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
        # TODO: waiter = client.get_waiter('table_exists')
        # TODO: waiter.wait(TableName=..., WaiterConfig={'Delay':  1})
