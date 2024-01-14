from decimal import Decimal
import json
import itertools
import logging
import sys
from typing import Any
from incline.base62 import base_encode
from incline.InclineDatastore import incline_resolve
from incline.InclineDatastoreDynamo import InclineDatastoreDynamo
from incline.InclineIndex import InclineIndex
from incline.InclineMeta import InclineMeta, InclineMetaWrite
from incline.InclinePrepare import InclinePrepare, InclinePxn
from incline.InclineRecord import InclineRecord
from incline.InclineResponse import InclineResponse
from incline.InclineTrace import InclineTrace
from incline.router import InclineRouterOne
from incline.error import InclineNotFound, InclineInterface


class InclineClient(object):

    def __init__(self,
                 name: str = 'incline',
                 region: str = 'us-west-2',
                 cid: str | None = None,
                 uid: str | None = None,
                 rid: str | None = None,
                 trace: InclineTrace | None = None):
        """
        cid: client Id
        uid: user Id
        rid: request Id
        """
        self.name = name
        self.region = region
        self.prepare = InclinePrepare(cid=cid)
        self.__uid = uid
        self.__rid = rid
        self.rtr = InclineRouterOne(name=self.name, region=self.region)
        self.cons: list[InclineDatastoreDynamo] = list()
        self.indexes: dict[str, InclineIndex] = {}

        # Tracing
        self.trace = trace
        if not self.trace:
            self.trace = InclineTrace(name=name)

        # Logging
        self.log = logging.getLogger('incline.client.' + self.name)
        if not self.__log_configured():
            self.logfmt = 'i.client ({0}|{1}) rid={2} cid={3} uid={4} %(message)s'.format(
                self.name, self.region, self.__rid, self.prepare.cid(),
                self.__uid)
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(self.logfmt)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

    def __log_configured(self) -> bool:
        for h in self.log.handlers:
            if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout:
                return True
        return False

    @property
    def rid(self) -> str | None:
        return self.__rid

    @rid.setter
    def rid(self, val: str) -> str:
        if val:
            self.__rid = val
        if not self.__rid:
            self.__rid = '0'
        return self.__rid

    @property
    def uid(self) -> str | None:
        return self.__uid

    @uid.setter
    def uid(self, val: str) -> str:
        if val:
            self.__uid = val
        if not self.__uid:
            self.__uid = '0'
        return self.__uid

    def get(self, keys: list[str] | str) -> InclineResponse:
        vals = dict()
        if not keys:
            raise InclineInterface('client get with no keys')
        if not isinstance(keys, list):
            keys = [keys]

        self.log.info('get [%s]', ','.join(keys))

        pxn = InclinePxn(cid=0, cnt=0)

        # Round 1 - GET highest commit for each key
        for k in keys:
            val = self.getkey(k)
            vals[val.kid] = val

            # preserve highest pxn for response
            if val.pxn > pxn:
                pxn = val.pxn

        # Round 2 - Resolve inconsistencies
        for v in vals.values():
            for m in v.met.meta:
                # 2.1 - Verify each val metadata older than other vals in set
                if m.kid in vals and (vals[m.kid].pxn < m.pxn):
                    self.log.warning(f"get readatomic {m.kid} {m.loc} {m.pxn}")
                    # 2.2 - GET from LOG any missing newer keys
                    vals[m.kid] = self.getlog(m.kid, m.loc, m.pxn)

                    # preserve highest pxn for response
                    if val.pxn > pxn:
                        pxn = val.pxn

        resp = InclineResponse(pxn=pxn)
        for k, v in vals.items():
            resp.data[k] = v
        return resp

    def put(self, kid: str, dat: dict[str, Any]) -> InclineResponse:
        self.log.info('put %s', kid)
        resp = self.putatomic([{'kid': kid, 'dat': dat}])
        return resp

    def puts(self, dat: list[dict[str, Any]]) -> InclineResponse:
        """
        [{'kid': kid, 'dat': dat}]
        """
        self.log.info('puts %d', len(dat))
        resp = self.putatomic(dat)
        return resp

    def search(self) -> None:
        pass

    def create(self, kid: str, dat: dict[str, Any]) -> InclineResponse:
        self.log.info('create %s', kid)
        resp = self.putatomic([{'kid': kid, 'dat': dat}], mode='create')
        return resp

    def creates(self, dat: list[dict[str, Any]]) -> InclineResponse:
        """
        [{'kid': kid, 'dat': dat}]
        """
        self.log.info('creates %s', len(dat))
        resp = self.putatomic(dat, mode='create')
        return resp

    def delete(self, kid: str) -> InclineResponse:
        """
        Delete creates with empty data, which causes a tombstone record to be
        created.  Reads filter tombstones earlier than now.
        """
        self.log.info('delete %s', kid)
        resp = self.putatomic([{'kid': kid, 'dat': {}}], mode='delete')
        return resp

    def history(self,
                key: str,
                tsv: Decimal | None = None,
                limit: int = 0) -> InclineResponse:
        """
        History returns a page of history starting at tsv or most recent

        tsv     - timestamp less than or equal to
        limit   - zero is unlimited, otherwise the number of items to return
        """
        datastores = self.rtr.lookup('read', key)
        self.log.info('history %s [%s]', key, ','.join(datastores))
        vals: list[InclineRecord] = list()
        for ds in datastores:
            con = self.ds_open(ds)
            val = con.get(key, tsv=tsv, limit=limit)
            for v in val:
                vals.append(v)
        if not vals:
            raise InclineNotFound('key not found in any datastore')

        # sort newest -> oldest
        vals.sort(key=lambda x: x.tsv, reverse=True)

        # set response pxn to the first (newest) record
        first = next(iter(vals))
        resp = InclineResponse(pxn=first.pxn)
        for v in vals:
            resp.data[str(v.tsv)] = v

        return resp

    def getkey(self, key: str) -> InclineRecord:
        datastores = self.rtr.lookup('read', key)
        self.log.info('getkey %s [%s]', key, ','.join(datastores))
        vals: list[InclineRecord] = list()
        for ds in datastores:
            con = self.ds_open(ds)
            val = con.get(key)
            for v in val:
                vals.append(v)
        if not vals:
            raise InclineNotFound('key not found in any datastore')
        return self.verify(vals)

    def getlog(self, key: str, loc: str, pxn: InclinePxn) -> InclineRecord:
        self.log.info('getlog %s %s %s', key, loc, format(pxn))
        vals: list[InclineRecord] = list()
        con = self.ds_open(loc)
        val = con.get(key, pxn=pxn)
        for v in val:
            vals.append(v)
        if not vals:
            raise InclineNotFound('log not found in any datastore')
        return self.verify(vals)

    def putatomic(self,
                  dat: list[dict[str, Any]],
                  mode: str | None = None) -> InclineResponse:
        """
        InclineResponse includes a list of all InclineRecord commits to all
        datastores
        """
        # TODO: check number ranges and data types (ex: dynamo decimal)
        datastores = list()
        pxn = self.prepare.pxn()

        for d in dat:
            self.log.info('putatomic %s %s', d['kid'], format(pxn))
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

        commits = []
        for ds in datastores:
            con = self.ds_open(ds)
            for d in dat:
                if ds in d['datastores']:
                    commit = con.commit(d['kid'], pxn, mode=mode)
                    commits += commit

        resp = InclineResponse(pxn=pxn)
        for c in commits:
            resp.data[c.kid] = c
        return resp

    def refresh(self, key: str) -> InclineResponse:
        """
        InclineResponse includes a list of all InclineRecord commits to all
        datastores
        """
        # TODO: check number ranges and data types (ex: dynamo decimal)
        txn = self.getkey(key)

        self.log.info('refresh %s %s', txn.kid, format(txn.pxn))
        datastores = self.rtr.lookup('write', txn.kid)

        commits = []
        for ds in datastores:
            con = self.ds_open(ds)

            commit = None

            # refresh from log if present
            try:
                commit = con.refresh(kid=txn.kid, pxn=txn.pxn)
            except InclineNotFound as e:
                self.log.info(f"refresh {txn.kid} pxn {format(txn.pxn)} " \
                        f"log not found, refresh from tsv {txn.tsv}")
                pass

            # refresh from txn if log gone
            if not commit:
                commit = con.refresh(kid=txn.kid, tsv=txn.tsv)
            commits += commit

        resp = InclineResponse(pxn=txn.pxn)
        for c in commits:
            resp.data[c.kid] = c
        return resp

    def index(self, idx: str, val: Any) -> list[dict[str, Any]]:
        datastores = self.rtr.lookup('index', idx)
        self.log.info('index %s %s [%s]', idx, val, ','.join(datastores))
        vals: list[dict[str, Any]] = []
        for ds in datastores:
            con = self.ds_open(ds)
            items = con.ds_get_idx(idx, val)
            for i in items:
                vals.append(i)
        if not vals:
            raise InclineNotFound('idx val not found in any datastore index')

        return vals

    def genmet(self,
               datastores: list[str],
               datastore: str,
               kid: str,
               pxn: InclinePxn,
               dat: list[dict[str, Any]] = []) -> InclineMeta:
        """
        Generate metadata
        """
        met = InclineMeta()
        for ds in datastores:
            con = self.ds_open(ds)

            # Do not add self
            if ds == datastore:
                continue

            met.add_write(InclineMetaWrite(kid, con.loc(), pxn))

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
                met.add_write(InclineMetaWrite(d['kid'], con.loc(), pxn))

        return met

    def verify(self, vals: list[InclineRecord]) -> InclineRecord:
        """
        Compare values returned from multiple sources, log errors, and return
        the newest value.
        """
        if len(vals) == 1:
            return vals[0]
        for v1, v2 in itertools.combinations(vals, 2):
            val = v1
            if v1.tsv < v2.tsv:
                val = v2
            if v1.dat != v2.dat:
                print('client validation error')
                print('A: {0}'.format(v1))
                print('B: {0}'.format(v2))
        return val

    def set_index(self, index: InclineIndex) -> None:
        """
        Promote a nested dat.x.y.z path to the top level to enable Global
        Secondary Indexes.  Prefix with idx_
        """
        if not index.name:
            raise InclineInterface("invalid index with no name")
        self.indexes[index.name] = index

        for c in self.cons:
            if index.name not in c.indexes:
                c.set_index(index)

    def ds_find(self, location: str) -> InclineDatastoreDynamo | None:
        for c in self.cons:
            if self.ds_equal(c, location):
                return c
        return None

    def ds_equal(self, con: InclineDatastoreDynamo, location: str) -> bool:
        loc = incline_resolve(location)
        return bool(con.dbtype == loc['dbtype'] and \
                con.region == loc['region'] and \
                con.name == loc['name'])

    def ds_open(self, location: str) -> InclineDatastoreDynamo:
        con = self.ds_find(location)
        if con:
            return con

        self.log.info('dsopen %s', location)
        loc = incline_resolve(location)
        if loc['dbtype'] == 'dynamo':
            con = InclineDatastoreDynamo(name=loc['name'],
                                         region=loc['region'],
                                         trace=self.trace)
            con.rid(rid=self.__rid)
            con.uid(uid=self.__uid)
            con.pxn.cid(self.prepare.cid())
        else:
            raise InclineInterface('unknown datastore in location string')

        for name, index in self.indexes.items():
            con.set_index(index)

        self.cons.append(con)
        return con
