from incline.error import (InclineError, InclineExists, InclineDataError,
                           InclineNotFound, InclineInterface)
from incline.base62 import base_encode
from incline.flatten import flatten
from incline.InclinePrepare import InclinePrepare
from incline.InclinePrepare import (INCLINE_TXN_MULTIPLY, INCLINE_TXN_QUANTIZE,
                                    INCLINE_TXN_BASEJUST)
from incline.InclineTrace import InclineTrace
from decimal import Decimal
import logging
import sys
from typing import Any
from opentelemetry.trace.span import Span


def incline_resolve(location: str, delimiter: str = '|') -> dict[str, str]:
    """
    <type>|<region>|<name>
    """
    parts = location.split(delimiter)
    if len(parts) != 3:
        raise InclineInterface('location string incorrect format')
    return {'dbtype': parts[0], 'region': parts[1], 'name': parts[2]}


class InclineDatastore(object):

    def __init__(self,
                 name: str = 'incline',
                 region: str = 'us-west-2',
                 trace: InclineTrace | None = None):
        self.init(name=name, region=region, trace=trace)

    def init(self,
             name: str,
             region: str,
             dbtype: str = 'none',
             trace: InclineTrace | None = None) -> None:
        self.region = region
        self.name = name
        self.dbtype = dbtype
        self.delimiter = '|'
        self.version = 1
        self.pxn = InclinePrepare()
        self.__uid = ""
        self.__rid = ""

        # Tracing
        if not trace:
            trace = InclineTrace(name=name)
        self.trace = trace

        # Logging
        self.log = logging.getLogger('incline.datastore.' + self.name)
        if not self.__log_configured():
            self.logfmt = 'i.datastore {0} rid={1} cid={2} uid={3} %(message)s'.format(
                self.loc(), self.__rid, self.pxn.cid(), self.__uid)
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

    def get(self,
            kid: str,
            tsv: Decimal | None = None,
            pxn: str | None = None) -> list[dict[str, Any]]:
        request_args = locals()
        with self.trace.span("incline.get") as span:
            self.map_request_span(request_args, span)
            if tsv:
                self.log.info('get %s tsv %s', kid, tsv)
                return self.ds_get_txn(kid, tsv)
            elif pxn:
                self.log.info('get %s pxn %s', kid, pxn)
                return self.ds_get_log(kid, pxn)

            self.log.info('get %s', kid)
            return self.filter_deleted(self.ds_get_txn(kid), tsv=tsv)

    def filter_deleted(self,
                       txns: list[dict[str, Any]] | dict[str, Any],
                       tsv: Decimal | None = None) -> list[dict[str, Any]]:
        """
        Return a list with any deleted records removed
        """
        with self.trace.span("incline.filter_deleted") as span:
            if not isinstance(txns, list):
                txns = [txns]

            if not tsv:
                tsv = self.pxn.now()

            results = list()
            for t in txns:
                if self.is_txn_deleted(t, tsv):
                    continue
                results.append(t)

            return results

    def prepare_val(self, kid: str, pxn: str, met: list[str | dict[str, Any]],
                    dat: dict[str, Any]) -> dict[str, Any]:
        return {
            'kid': kid,
            'pxn': pxn,
            'tsv': self.pxn.now(),
            'cid': self.pxn.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'ver': self.version,
            'met': self.canon_metadata(met),
            'dat': dat
        }

    def prepare(self, kid: str, pxn: str, met: list[str | dict[str, Any]],
                dat: dict[str, Any]) -> list[dict[str, Any]]:
        request_args = locals()
        with self.trace.span("incline.prepare") as span:
            self.map_request_span(request_args, span)
            self.log.info('prepare %s pxn %s', kid, pxn)
            val = self.prepare_val(kid, pxn, met, dat)
            return self.ds_prepare(kid, val)

    def commit(self, kid: str, pxn: str, mode: str | None = None) -> list[dict[str, Any]]:
        request_args = locals()
        with self.trace.span("incline.commit") as span:
            self.map_request_span(request_args, span)
            # Read log entry
            log = self.only(self.ds_get_log(kid, pxn))
            self.log.info('commit %s pxn %s org %s', kid, pxn, log['tsv'])
            return self.ds_commit(kid, log, mode=mode)

    def setup(self) -> None:
        with self.trace.span("incline.setup") as span:
            return self.ds_setup()

    def genlog(self, kid: str, pxn: str, met: list[str | dict[str, Any]],
               dat: dict[str, Any]) -> dict[str, Any]:
        log = {
            'kid': kid,
            'pxn': pxn,
            'tsv': self.pxn.now(),
            'cid': self.pxn.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'ver': self.version,
            'met': self.canon_metadata(met),
            'dat': dat
        }
        return log

    def gentxn(self, log: dict[str, Any], tsv: Decimal | int = Decimal(0)) -> dict[str, Any]:
        # Set tombstone when empty data
        tmb = Decimal(0)
        if log['dat'] is None:
            tmb = self.pxn.decimal(log['tsv'])

        txn = {
            'kid': log['kid'],
            'tsv': log['tsv'],
            'pxn': log['pxn'],
            'tmb': tmb,
            'cid': self.pxn.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'org': tsv,
            'ver': self.version,
            'met': log['met'],
            'dat': log['dat']
        }

        # InclineDatastoreDynamo/create zeroes tsv
        #if txn['tsv'] == 0:
        #    txn['tsv'] = self.pxn.now()

        return txn

    """
    Return a qualified location string
    """

    def loc(self) -> str:
        return '{0}{1}{2}{3}{4}'.format(self.dbtype, self.delimiter,
                                        self.region, self.delimiter, self.name)

    """
    Return a metadata item
    """

    def meta(self, kid: str, pxn: str = '0', loc: str | None = None) -> dict[str, Any]:
        if not loc:
            loc = self.loc()
        return {'kid': kid, 'loc': loc, 'pxn': pxn}

    """
    Fully qualify metadata with DB type and name
    """

    def canon_metadata(self, met: list[str | dict[str, Any]]) -> list[dict[str, Any]]:
        metadata: list[dict[str, Any]] = list()
        if not met:
            return metadata
        if not isinstance(met, list):
            raise InclineInterface('invalid metadata')
        for m in met:
            if isinstance(m, str):
                val = self.meta(m)
            elif isinstance(m, dict):
                pxn = '0'
                loc = None
                if 'kid' not in m:
                    raise InclineInterface('metadata missing key id')
                if 'pxn' in m:
                    pxn = m['pxn']
                if 'loc' in m:
                    loc = m['loc']
                val = self.meta(m['kid'], pxn=pxn, loc=loc)
            else:
                raise InclineInterface('metadata includes invalid type')
            metadata.append(val)
        return metadata

    def uid(self, uid: str | None = None) -> str:
        """
        User ID.  Provided by client, or '0'
        """
        if uid:
            self.__uid = uid
        # TODO: can we detect userid?
        if not self.__uid:
            self.__uid = '0'
        return self.__uid

    def rid(self, rid: str | None = None) -> str:
        """
        Request ID.  Provided by client, or '0'
        """
        if rid:
            self.__rid = rid
        if not self.__rid:
            self.__rid = '0'
        return self.__rid

    def numbers_to_remote(self, val: Any) -> Any:
        """
        Convert numbers for remote datastore.  Default is float to Decimal
        """
        if isinstance(val, float):
            val = self.pxn.decimal(f"{val}")
        elif isinstance(val, list):
            for i in range(len(val)):
                val[i] = self.numbers_to_remote(val[i])
        elif isinstance(val, dict):
            for k in val.keys():
                val[k] = self.numbers_to_remote(val[k])
        return val

    def numbers_to_local(self, val: Decimal | float | int | list[Any] | dict[Any, Any]) -> Any:
        """
        Convert numbers to local.  Default is Decimal to float/int
        """
        if isinstance(val, Decimal):
            tuple = val.as_tuple()
            if abs(int(tuple.exponent)) > 0:
                val = float(val)
            else:
                val = int(val)
        elif isinstance(val, list):
            for i in range(len(val)):
                val[i] = self.numbers_to_local(val[i])
        elif isinstance(val, dict):
            for k in val.keys():
                val[k] = self.numbers_to_local(val[k])
        return val

    def only(self, val: list[Any]) -> Any:
        """
        Return the first, only, item from a list
        """
        if len(val) == 0:
            return None
        if len(val) != 1:
            raise InclineDataError(f"only cannot be {len(val)} objects")
        return val[0]

    def first(self, val: Any) -> Any:
        """
        Return the first item from a list
        """
        if len(val) == 0:
            return None
        return val[0]

    def map_log_response(self, resp: list[dict[str, Any]] | dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(resp, list):
            resp = [resp]
        results = list()
        for r in resp:
            if 'ver' not in r:
                continue
            if int(r['ver'] == 1):
                r = self.map_log_response_v1(r)
            if r:
                results.append(r)
        return results

    def map_txn_response(self, resp: list[dict[str, Any]] | dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(resp, list):
            resp = [resp]
        results = list()
        for r in resp:
            if 'ver' not in r:
                continue
            if int(r['ver'] == 1):
                r = self.map_txn_response_v1(r)
            if r:
                results.append(r)
        return results

    def map_log_response_v1(self, resp: dict[str, Any]) -> dict[str, Any]:
        keys = ['kid', 'pxn', 'tsv', 'cid', 'uid', 'rid', 'ver', 'met', 'dat']
        r = dict()
        for k in keys:
            if k in resp:
                if k == 'ver':
                    r[k] = int(resp[k])
                else:
                    r[k] = resp[k]
        return r

    def map_txn_response_v1(self, resp: dict[str, Any]) -> dict[str, Any]:
        keys = [
            'kid', 'tsv', 'pxn', 'tmb', 'cid', 'uid', 'rid', 'org', 'ver',
            'met', 'dat'
        ]
        r = dict()
        for k in keys:
            if k in resp:
                if k == 'ver':
                    r[k] = int(resp[k])
                elif k == 'tsv':
                    r[k] = self.pxn.decimal(resp[k])
                elif k == 'tmb':
                    r[k] = self.pxn.decimal(resp[k])
                else:
                    r[k] = resp[k]
        return r

    def map_request_span(self, value: Any, span: Span) -> None:
        """
        map a dict of arguments into span attributes
        filter out 'dat' to avoid tracing stored data
        """
        flat = flatten(value, prefix="request")
        for k, v in flat.items():
            if k == 'dat' or ".dat." in k or k.endswith(".dat"):
                continue
            # drop class self local
            if k == 'self' or ".self." in k or k.endswith(".self"):
                continue
            # string instead of float losing precision for Decimal
            if isinstance(v, Decimal):
                v = str(v)
            # cannot set span attribute to None
            if v == None:
                continue
            span.set_attribute(k, v)

    def map_response_span(self, value: Any, span: Span) -> None:
        """
        map a dict of arguments into span attributes
        filter out 'dat' to avoid tracing stored data
        """
        flat = flatten(value, prefix="response")
        for k, v in flat.items():
            if k == 'dat' or ".dat." in k or k.endswith(".dat"):
                continue
            # drop class self local
            if k == 'self' or ".self." in k or k.endswith(".self"):
                continue
            # string instead of float losing precision for Decimal
            if isinstance(v, Decimal):
                v = str(v)
            # cannot set span attribute to None
            if v == None:
                continue
            span.set_attribute(k, v)

    def map_txn_span(self, value: Any, span: Span, prefix: str = "response") -> None:
        """
        map a dict of arguments into span attributes
        filter out 'dat' to avoid tracing stored data
        """
        flat = flatten(value, prefix=prefix)
        for k, v in flat.items():
            if k == 'dat' or ".dat." in k or k.endswith(".dat"):
                continue
            # string instead of float losing precision for Decimal
            if isinstance(v, Decimal):
                v = str(v)
            # cannot set span attribute to None
            if v == None:
                continue
            span.set_attribute(k, v)

    def map_log_span(self, value: Any, span: Span, prefix: str = "response") -> None:
        """
        map a dict of arguments into span attributes
        filter out 'dat' to avoid tracing stored data
        """
        flat = flatten(value, prefix=prefix)
        for k, v in flat.items():
            if k == 'dat' or ".dat." in k or k.endswith(".dat"):
                continue
            # string instead of float losing precision for Decimal
            if isinstance(v, Decimal):
                v = str(v)
            # cannot set span attribute to None
            if v == None:
                continue
            span.set_attribute(k, v)

    def is_txn_deleted(self,
                       txn: dict[str, Any],
                       tsv: Decimal | None = None) -> bool:
        """
        Record is deleted when a tombstone exists in the past
        """
        # no record is considered deleted
        if not txn:
            return True

        if not tsv:
            tsv = self.pxn.now()

        tmb = txn.get('tmb', 0)
        return bool(tmb != 0 and tmb < tsv)

    """
    Methods to override
    """

    def ds_get_log(self, kid: str, pxn: str | None = None) -> list[dict[str, Any]]:
        return []

    def ds_get_txn(self, kid: str, tsv: Decimal | None = None, limit: int = 1) -> list[dict[str, Any]]:
        return []

    def ds_prepare(self, kid: str, val: dict[str, Any]) -> list[dict[str, Any]]:
        return []

    def ds_commit(self, kid: str, log: dict[str, Any], mode: str | None = None) -> list[dict[str, Any]]:
        return []

    def ds_scan_log(self, kid: str | None = None, tsv: Decimal | None = None,
                    limit: int | None = None) -> list[dict[str, Any]]:
        return []

    def ds_scan_txn(self,
                    kid: str | None = None,
                    tsv: Decimal | int | str | None = None,
                    limit: int | None = None) -> list[dict[str, Any]]:
        return []

    def ds_delete_log(self, kid: str, pxn: str) -> None:
        pass

    def ds_delete_txn(self, kid: str, tsv: Decimal) -> None:
        pass

    def ds_setup(self) -> None:
        pass
