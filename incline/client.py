import datetime
import uuid
import decimal
import numbers
import json
import itertools
import logging
import sys
from base62 import base_encode
from datastore import incline_resolve
from datastoreDdb import InclineDatastoreDdb
from router import InclineRouterOne
from error import InclineNotFound
from pprint import pprint


class InclineClient(object):

    def __init__(self,
                 name='incline',
                 region='us-east-1',
                 cid=None,
                 uid=None,
                 rid=None
                 ):
        """
        cid: client Id
        uid: user Id
        rid: request Id
        """
        self.name = name
        self.region = region
        self.__cid = cid
        self.__uid = uid
        self.__rid = rid
        self.__tsv = 0
        self.__tsv = int(self.now() * 1000000)
        self.__cnt = self.__tsv
        self.rtr = InclineRouterOne(name=self.name, region=self.region)
        self.cons = list()

        # Logging
        self.log = logging.getLogger('incline.client.' + self.name)
        if not self.__log_configured():
            self.logfmt = 'i.client ({0}|{1}) rid={2} cid={3} uid={4} %(message)s'.format(
                self.name, self.region, self.__rid, self.cid(), self.__uid)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(self.logfmt)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

    def __log_configured(self):
        for h in self.log.handlers:
            if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout:
                return True
        return False

    def get(self, keys):
        vals = dict()
        if not keys:
            raise InclineInterface('client get with no keys')
        if not isinstance(keys, list):
            keys = [keys]

        self.log.info('get [%s]', ','.join(keys))

        # Round 1 - GET highest commit for each key
        for k in keys:
            val = self.getkey(k)
            vals[val['kid']] = val

        # Round 2 - Resolve inconsistencies
        for v in vals.values():
            for m in v['met']:
                # 2.1 - Verify each val metadata older than other vals in set
                if m['kid'] in vals and self.cmppxn(
                        vals[m['kid']]['pxn'], m['pxn']) < 0:
                    self.log.warn('get readatomic %s %s %s',
                                  m['kid'], m['loc'], m['pxn'])
                    # 2.2 - GET from LOG any missing newer keys
                    vals[m['kid']] = self.getlog(m['kid'], m['loc'], m['pxn'])

        return vals

    def put(self, kid, dat):
        self.log.info('put %s', kid)
        pxn = self.putatomic([{'kid': kid, 'dat': dat}])
        return pxn

    def puts(self, dat):
        """
        [{'kid': kid, 'dat': dat}]
        """
        self.log.info('puts %d', len(dat))
        pxn = self.putatomic(dat)
        return pxn

    def search(self):
        pass

    def create(self, kid, dat):
        self.log.info('create %s', kid)
        pxn = self.putatomic([{'kid': kid, 'dat': dat}], create=True)
        return pxn

    def creates(self, dat):
        """
        [{'kid': kid, 'dat': dat}]
        """
        self.log.info('creates %s', len(dat))
        pxn = self.putatomic(dat, create=True)
        return pxn

    def getkey(self, key):
        datastores = self.rtr.lookup('read', key)
        self.log.info('getkey %s [%s]', key, ','.join(datastores))
        vals = list()
        for ds in datastores:
            con = self.ds_open(ds)
            val = con.get(key)
            for v in val:
                vals.append(v)
        if not vals:
            raise InclineNotFound('key not found in any datastore')
        return self.verify(vals)

    def getlog(self, key, loc, pxn):
        self.log.info('getlog %s %s %s', key, loc, pxn)
        vals = list()
        con = self.ds_open(loc)
        val = con.get(key, pxn=pxn)
        for v in val:
            vals.append(v)
        if not vals:
            raise InclineNotFound('log not found in any datastore')
        return self.verify(vals)

    def putatomic(self, dat, create=False):
        datastores = list()
        pxn = self.pxn()

        for d in dat:
            self.log.info('putatomic %s %s', d['kid'], pxn)
            # per-item datastore list
            d['datastores'] = self.rtr.lookup('write', d['kid'])
            datastores.extend(d['datastores'])

        # unique list of datastores
        datastores = list(set(datastores))

        for ds in datastores:
            con = self.ds_open(ds)
            for d in dat:
                if ds in d['datastores']:
                    met = self.genmet(d['datastores'], ds, d['kid'], pxn, dat)
                    con.prepare(d['kid'], pxn, met, d['dat'])

        for ds in datastores:
            con = self.ds_open(ds)
            for d in dat:
                if ds in d['datastores']:
                    con.commit(d['kid'], pxn, create=create)

        return pxn

    def genmet(self, datastores, datastore, kid, pxn, dat=[]):
        met = list()
        for ds in datastores:
            con = self.ds_open(ds)

            # Do not add self
            if ds == datastore:
                continue

            met.append(con.meta(kid, pxn))

        # Add metadata for additional keys
        for d in dat:
            # Do not add self
            if d['kid'] == kid:
                continue

            for ds in d['datastores']:
                con = self.ds_open(ds)
                met.append(con.meta(d['kid'], pxn))

        return met

    def cmpval(self, val1, val2):
        if val1['dat'] == val2['dat']:
            return True
        return False

    def verify(self, vals):
        """
        Compare values returned from multiple sources, log errors, and return
        the newest value.
        """
        val = None
        if len(vals) == 1:
            return vals[0]
        for v1, v2 in itertools.combinations(vals, 2):
            if not val:
                val = v1
            if v1['tsv'] < v2['tsv']:
                val = v2
            if not self.cmpval(v1, v2):
                print 'client validation error'
                print 'A: {0}'.format(self.strval(k1))
                print 'B: {0}'.format(self.strval(k2))
        return val

    def strval(self, val):
        return 'kid={0} tsv={1} pxn={2}'.format(
            val['kid'],
            val['tsv'],
            val['pxn'])

    def ds_find(self, location):
        for c in self.cons:
            if self.ds_equal(c, location):
                return c
        return None

    def ds_equal(self, con, location):
        loc = incline_resolve(location)
        return con.dbtype == loc['dbtype'] and \
            con.region == loc['region'] and \
            con.name == loc['name']

    def ds_open(self, location):
        con = self.ds_find(location)
        if con:
            return con

        self.log.info('dsopen %s', location)
        loc = incline_resolve(location)
        if loc['dbtype'] == 'ddb':
            con = InclineDatastoreDdb(
                name=loc['name'],
                region=loc['region']
            )
            con.rid(rid=self.__rid)
            con.uid(uid=self.__uid)
            con.cid(cid=self.__cid)
        else:
            raise InclineInterfaceError('unknown datastore in location string')

        self.cons.append(con)
        return con

    def pxn(self):
        """
        Prepare Transaction ID.
        Timestamps should be unique across transactions, and for session
        consistency, increase on a per-client basis. Given unique client IDs, a
        client ID and sequence number form unique transaction timestamps
        without coordination.  Use base62 encoding, padded to 11 digits
        """
        pxn = '{0}.{1}'
        return pxn.format(
            self.cid(),
            base_encode(self.cnt()).rjust(11, '0')
        )

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

    def cid(self, cid=None):
        """
        Client ID. Provided by client, MAC address or random number.  Encode
        base-62 if CID is a number.
        """
        if not self.__cid:
            if not cid:
                cid = uuid.getnode()
            if isinstance(cid, numbers.Number):
                self.__cid = base_encode(cid)
            else:
                self.__cid = cid
        return self.__cid

    def cnt(self, cnt=None):
        """
        Monotonic timestamp counter
        """
        now = int(self.now() * 1000000)
        if cnt < now:
            cnt = now
        if cnt < self.__cnt:
            cnt = self.__cnt + 1
        self.__cnt = cnt
        return self.__cnt

    def now(self):
        """
        Monotonic microsecond UTC timestamp
        """
        now = decimal.Decimal(datetime.datetime.utcnow().strftime('%s.%f'))
        if now <= self.__tsv:
            now = self.__tsv + decimal.Decimal('0.000001')
        self.__tsv = now
        return now
