from error import InclineError, InclineInterface, InclineDataError
from base62 import base_encode
import uuid
import datetime
import decimal
import numbers
import logging
import sys


def incline_resolve(location, delimiter='|'):
    """
    <type>|<region>|<name>
    """
    parts = location.split(delimiter)
    if len(parts) != 3:
        raise InclineInterface('location string incorrect format')
    return {
        'dbtype': parts[0],
        'region': parts[1],
        'name': parts[2]
    }


class InclineDatastore(object):

    def __init__(self, name='incline', region='us-east-1'):
        self.init(name=name, region=region)

    def init(self, name, region, dbtype='none'):
        self.region = region
        self.name = name
        self.dbtype = dbtype
        self.delimiter = '|'
        self.version = 1
        self.__cid = None
        self.__uid = None
        self.__rid = None
        self.__tsv = decimal.Decimal(0)

        # Logging
        self.log = logging.getLogger('incline.datastore.' + self.name)
        if not self.__log_configured():
            self.logfmt = 'i.datastore {0} rid={1} cid={2} uid={3} %(message)s'.format(
                self.loc(), self.__rid, self.__cid, self.__uid)
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

    def get(self, kid, tsv=None, pxn=None):
        if tsv:
            self.log.info('get %s tsv %s', kid, tsv)
            return self.ds_get_txn(kid, tsv)
        elif pxn:
            self.log.info('get %s pxn %s', kid, pxn)
            return self.ds_get_log(kid, pxn)

        self.log.info('get %s', kid)
        return self.ds_get_txn(kid)

    def prepare(self, kid, pxn, met, dat):
        self.log.info('prepare %s pxn %s', kid, pxn)
        val = {
            'kid': kid,
            'pxn': pxn,
            'tsv': self.now(),
            'cid': self.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'ver': self.version,
            'met': self.canon_metadata(met),
            'dat': dat
        }
        return self.ds_prepare(kid, val)

    def commit(self, kid, pxn, create=False):
        # Read log entry
        log = self.only(self.ds_get_log(kid, pxn))
        self.log.info('commit %s pxn %s org %s', kid, pxn, log['tsv'])
        return self.ds_commit(kid, log, create=create)

    def setup(self):
        return self.ds_setup()

    def genlog(self, kid, pxn, met, dat):
        log = {
            'kid': kid,
            'pxn': pxn,
            'tsv': self.now(),
            'cid': self.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'ver': self.version,
            'met': self.canon_metadata(met),
            'dat': dat
        }
        return log

    def gentxn(self, log, tsv=0):
        # Set tombstone when empty data
        tmb = False
        if log['dat'] is None:
            tmb = True

        txn = {
            'kid': log['kid'],
            'tsv': log['tsv'],
            'pxn': log['pxn'],
            'tmb': tmb,
            'cid': self.cid(),
            'uid': self.uid(),
            'rid': self.rid(),
            'org': tsv,
            'ver': self.version,
            'met': log['met'],
            'dat': log['dat']
        }
        return txn

    """
    Return a qualified location string
    """

    def loc(self):
        return '{0}{1}{2}{3}{4}'.format(
            self.dbtype, self.delimiter,
            self.region, self.delimiter,
            self.name)

    """
    Return a metadata item
    """

    def meta(self, kid, pxn='0', loc=None):
        if not loc:
            loc = self.loc()
        return {
            'kid': kid,
            'loc': loc,
            'pxn': pxn
        }

    """
    Fully qualify metadata with DB type and name
    """

    def canon_metadata(self, met):
        metadata = list()
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

    def uid(self, uid=None):
        """
        User ID.  Provided by client, or '0'
        """
        if not self.__uid:
            if uid:
                self.__uid = uid
            else:
                self.__uid = '0'
        return self.__uid

    def rid(self, rid=None):
        """
        Request ID.  Provided by client, or '0'
        """
        if not self.__rid:
            if rid:
                self.__rid = rid
            else:
                self.__rid = '0'
        return self.__rid

    def now(self):
        """
        Monotonic microsecond UTC timestamp
        """
        now = decimal.Decimal(datetime.datetime.utcnow().strftime('%s.%f'))
        if now <= self.__tsv:
            now = self.__tsv + decimal.Decimal('0.000001')
        self.__tsv = now
        return now

    def only(self, val):
        """
        Return the first, only, item from a list
        """
        if len(val) == 0:
            return None
        if len(val) != 1:
            raise InclineDataError('only cannot be multiple objects')
        return val[0]

    def first(self, val):
        """
        Return the first item from a list
        """
        if len(val) == 0:
            return None
        return val[0]

    """
    Methods to override
    """

    def ds_get_log(self, kid, pxn=None):
        pass

    def ds_get_txn(self, kid, tsv=None, limit=1):
        pass

    def ds_prepare(self, kid, val):
        pass

    def ds_commit(self, kid, log, create=False):
        pass

    def ds_setup(self):
        pass
