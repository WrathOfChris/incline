import decimal
import json
import itertools
import logging
import sys
from incline.base62 import base_encode
from incline.InclineDatastore import incline_resolve
from incline.InclineDatastoreDynamo import InclineDatastoreDynamo
from incline.InclinePrepare import InclinePrepare
from incline.router import InclineRouterOne
from incline.error import InclineNotFound


class InclineClient(object):

    def __init__(self,
                 name='incline',
                 region='us-west-2',
                 cid=None,
                 uid=None,
                 rid=None):
        """
        cid: client Id
        uid: user Id
        rid: request Id
        """
        self.name = name
        self.region = region
        self.pxn = InclinePrepare(cid=cid)
        self.__uid = uid
        self.__rid = rid
        self.rtr = InclineRouterOne(name=self.name, region=self.region)
        self.cons = list()

        # Logging
        self.log = logging.getLogger('incline.client.' + self.name)
        if not self.__log_configured():
            self.logfmt = 'i.client ({0}|{1}) rid={2} cid={3} uid={4} %(message)s'.format(
                self.name, self.region, self.__rid, self.pxn.cid(), self.__uid)
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
                if m['kid'] in vals and self.pxn.cmppxn(
                        vals[m['kid']]['pxn'], m['pxn']) < 0:
                    self.log.warn('get readatomic %s %s %s', m['kid'],
                                  m['loc'], m['pxn'])
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
        pxn = self.putatomic([{'kid': kid, 'dat': dat}], mode='create')
        return pxn

    def creates(self, dat):
        """
        [{'kid': kid, 'dat': dat}]
        """
        self.log.info('creates %s', len(dat))
        pxn = self.putatomic(dat, mode='create')
        return pxn

    def delete(self, kid):
        """
        Delete creates with empty data, which causes a tombstone record to be
        created.  Reads filter tombstones earlier than now.
        """
        self.log.info('delete %s', kid)
        pxn = self.putatomic([{'kid': kid, 'dat': None}], mode='delete')
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

    def putatomic(self, dat, mode=None):
        # TODO: check number ranges and data types (ex: dynamo decimal)
        datastores = list()
        pxn = self.pxn.pxn()

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
                    con.commit(d['kid'], pxn, mode=mode)

        return pxn

    def genmet(self, datastores, datastore, kid, pxn, dat=[]):
        """
        Generate metadata
        """
        met = list()
        for ds in datastores:
            con = self.ds_open(ds)

            # Do not add self
            if ds == datastore:
                continue

            met.append(con.meta(kid, pxn))

        # no data provided, delete/tombstone
        if not dat:
            return met

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
                print('client validation error')
                print('A: {0}'.format(self.strval(k1)))
                print('B: {0}'.format(self.strval(k2)))
        return val

    def strval(self, val):
        return 'kid={0} tsv={1} pxn={2}'.format(val['kid'], val['tsv'],
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
        if loc['dbtype'] == 'dynamo':
            con = InclineDatastoreDynamo(name=loc['name'],
                                         region=loc['region'])
            con.rid(rid=self.__rid)
            con.uid(uid=self.__uid)
            con.pxn.cid(cid=self.pxn.cid())
        else:
            raise InclineInterfaceError('unknown datastore in location string')

        self.cons.append(con)
        return con
