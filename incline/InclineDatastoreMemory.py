from incline.InclineDatastore import InclineDatastore
from incline.error import (InclineError, InclineExists, InclineDataError,
                           InclineNotFound)
import decimal
import copy

# global memory store
DATASTORE_MEMORY = dict()

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


class InclineDatastoreMemory(InclineDatastore):
    """
    LOG {
        'kid1': {
            'pxn1': {},
            'pxn2': {}
            },
        'kid2': {
            'pxn3': {},
            'pxn4': {}
            }
    }
    """

    def __init__(self, name='incline', region='us-west-2'):
        self.init(name, region, dbtype='memory')
        self.ds_init()

    def ds_init(self):
        global DATASTORE_MEMORY

        self.logname = self.name + '-log'
        self.txnname = self.name + '-txn'
        self.logdb = None
        self.txndb = None

        if self.logname not in DATASTORE_MEMORY:
            DATASTORE_MEMORY[self.logname] = dict()
        self.logdb = DATASTORE_MEMORY[self.logname]

        if self.txnname not in DATASTORE_MEMORY:
            DATASTORE_MEMORY[self.txnname] = dict()
        self.txndb = DATASTORE_MEMORY[self.txnname]

    def cmppxn(self, pxn1, pxn2):
        cid1 = pxn1.split('.')[0]
        cnt1 = pxn1.split('.')[1]
        cid2 = pxn2.split('.')[0]
        cnt2 = pxn2.split('.')[1]

        # Compare counter
        if cnt1 < cnt2:
            return -1
        elif cnt1 > cnt2:
            return 1

        # Compare clientID
        if cid1 < cid2:
            return -1
        elif cid1 > cid2:
            return 1

        # Same ClientID and Counter
        return 0

    def ds_get_log(self, kid, pxn=None):
        request_args = locals()
        with self.trace.span("incline.datastore.ds_get_log") as span:
            self.map_request_span(request_args, span)
            log = self.logdb.get(kid)
            if not log:
                return []
            if pxn:
                self.log.info('getlog %s', kid)
            else:
                self.log.info('getlog %s pxn %s', kid, pxn)
                # XXX max in prepare transaction id order (counter, client)
                pxn = max((l.split('.')[1], l.split('.')[0]) for l in log)
            return self.map_log_response(copy.deepcopy(log.get(pxn)))

    def ds_get_txn(self, kid, tsv=None, limit=1):
        request_args = locals()
        with self.trace.span("incline.datastore.ds_get_txn") as span:
            self.map_request_span(request_args, span)

            if tsv and not isinstance(tsv, decimal.Decimal):
                tsv = self.pxn.decimal(tsv)

            txn = self.txndb.get(kid)
            if not txn:
                return []
            if tsv:
                self.log.info('gettxn %s tsv %s', kid, tsv)
            else:
                self.log.info('gettxn %s', kid)
                if not txn:
                    raise ValueError(self.txndb)
                tsv = max(txn)
            return self.map_txn_response(copy.deepcopy(txn.get(tsv)))

    def ds_prepare(self, kid, val):
        request_args = locals()
        with self.trace.span("incline.datastore.ds_prepare") as span:
            self.map_request_span(request_args, span)
            if kid not in self.logdb:
                self.logdb[kid] = {}
            self.logdb[kid][val.get('pxn', 0)] = val
            return self.map_log_response(copy.deepcopy(val))

    def ds_commit(self, kid, log, create=False):
        request_args = locals()
        with self.trace.span("incline.datastore.ds_commit") as span:
            self.map_request_span(request_args, span)

            # Read current version for origin tsv
            orgtsv = 0
            org = self.only(self.ds_get_txn(kid))
            if org and 'tsv' in org:
                orgtsv = org['tsv']

            # exists, can't create
            # TODO: better exception
            if create:
                if kid in self.txndb:
                    raise ValueError

            val = self.gentxn(log, tsv=orgtsv)
            if kid not in self.txndb:
                self.txndb[kid] = {}
            self.txndb[kid][log.get('tsv', 0)] = log
            return self.map_txn_response(copy.deepcopy(val))

    def ds_scan_log(self, kid=None, tsv=None, limit=None):
        """
        return list of [{'kid': kid, 'tsv': tsv}]
        """
        request_args = locals()
        logs = list()
        with self.trace.span("incline.datastore.ds_scan_log") as span:
            self.map_request_span(request_args, span)

            if kid:
                keys = [kid]
            else:
                keys = self.logdb.keys()

            for key in keys:
                for k, v in self.logdb.get(key, {}).items():
                    logs.append({'kid': key, 'pxn': v.get('pxn')})

            return logs

    def ds_scan_txn(self, kid=None, tsv=None, limit=None):
        request_args = locals()
        txns = list()
        with self.trace.span("incline.datastore.ds_scan_txn") as span:
            self.map_request_span(request_args, span)

            if tsv and not isinstance(tsv, decimal.Decimal):
                tsv = self.pxn.decimal(tsv)

            if kid:
                keys = [kid]
            else:
                keys = self.txndb.keys()

            for key in keys:
                for k, v in self.txndb.get(key, {}).items():
                    txns.append({'kid': key, 'tsv': v.get('tsv')})

            return txns

    def ds_delete_log(self, kid, pxn):
        request_args = locals()
        with self.trace.span("incline.datastore.ds_delete_log") as span:
            self.map_request_span(request_args, span)
            
            if kid not in self.logdb:
                return
            if pxn not in self.logdb[kid]:
                return
            del self.logdb[kid][pxn]
            if not self.logdb[kid]:
                del self.logdb[kid]

    def ds_delete_txn(self, kid, tsv):
        request_args = locals()
        with self.trace.span("incline.datastore.ds_delete_txn") as span:
            self.map_request_span(request_args, span)

            if kid not in self.txndb:
                return
            if tsv not in self.txndb[kid]:
                return
            del self.txndb[kid][tsv]
            if not self.txndb[kid]:
                del self.txndb[kid]
