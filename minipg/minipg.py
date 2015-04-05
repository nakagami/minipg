##############################################################################
#The MIT License (MIT)
#
#Copyright (c) 2014, 2015 Hajime Nakagami
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
##############################################################################
# https://github.com/nakagami/minipg/

from __future__ import print_function
import sys
import socket
import decimal
import datetime
import time
import uuid
import collections
import binascii

VERSION = (0, 4, 5)
__version__ = '%s.%s.%s' % VERSION
apilevel = '2.0'
threadsafety = 1
paramstyle = 'format'

PY2 = sys.version_info[0] == 2

DEBUG = False

def DEBUG_OUTPUT(s):
    print(s, end=' \n', file=sys.stderr)

#-----------------------------------------------------------------------------
# http://www.postgresql.org/docs/9.3/static/protocol.html
# http://www.postgresql.org/docs/9.3/static/protocol-message-formats.html

# postgresql-9.3.5/src/include/catalog/pg_type.h
PG_TYPE_BOOL = 16
PG_TYPE_BYTEA = 17
PG_TYPE_CHAR = 18
PG_TYPE_NAME = 19
PG_TYPE_INT8 = 20
PG_TYPE_INT2 = 21
PG_TYPE_INT2VECTOR = 22
PG_TYPE_INT4 = 23
PG_TYPE_REGPROC = 24
PG_TYPE_TEXT = 25
PG_TYPE_OID = 26
PG_TYPE_TID = 27
PG_TYPE_XID = 28
PG_TYPE_CID = 29
PG_TYPE_VECTOROID = 30
PG_TYPE_JSON = 114
PG_TYPE_XML = 142
PG_TYPE_PGNODETREE = 194
PG_TYPE_POINT = 600
PG_TYPE_LSEG = 601
PG_TYPE_PATH = 602
PG_TYPE_BOX = 603
PG_TYPE_POLYGON = 604
PG_TYPE_LINE = 628
PG_TYPE_FLOAT4 = 700
PG_TYPE_FLOAT8 = 701
PG_TYPE_ABSTIME = 702
PG_TYPE_RELTIME = 703
PG_TYPE_TINTERVAL = 704
PG_TYPE_UNKNOWN = 705
PG_TYPE_CIRCLE = 718
PG_TYPE_CASH = 790
PG_TYPE_MACADDR = 829
PG_TYPE_INET = 869
PG_TYPE_CIDR = 650
PG_TYPE_NAMEARRAY = 1003
PG_TYPE_INT2ARRAY = 1005
PG_TYPE_INT4ARRAY = 1007
PG_TYPE_TEXTARRAY = 1009
PG_TYPE_ARRAYOID = 1028
PG_TYPE_FLOAT4ARRAY = 1021
PG_TYPE_ACLITEM = 1033
PG_TYPE_CSTRINGARRAY = 1263
PG_TYPE_BPCHAR = 1042
PG_TYPE_VARCHAR = 1043
PG_TYPE_DATE = 1082
PG_TYPE_TIME = 1083
PG_TYPE_TIMESTAMP = 1114
PG_TYPE_TIMESTAMPTZ = 1184
PG_TYPE_INTERVAL = 1186
PG_TYPE_TIMETZ = 1266
PG_TYPE_BIT = 1560
PG_TYPE_VARBIT = 1562
PG_TYPE_NUMERIC = 1700
PG_TYPE_REFCURSOR = 1790
PG_TYPE_REGPROCEDURE = 2202
PG_TYPE_REGOPER = 2203
PG_TYPE_REGOPERATOR = 2204
PG_TYPE_REGCLASS = 2205
PG_TYPE_REGTYPE = 2206
PG_TYPE_REGTYPEARRAY = 2211
PG_TYPE_UUID = 2950
PG_TYPE_TSVECTOR = 3614
PG_TYPE_GTSVECTOR = 3642
PG_TYPE_TSQUERY = 3615
PG_TYPE_REGCONFIG = 3734
PG_TYPE_REGDICTIONARY = 3769
PG_TYPE_INT4RANGE = 3904
PG_TYPE_RECORD = 2249
PG_TYPE_RECORDARRAY = 2287
PG_TYPE_CSTRING = 2275
PG_TYPE_ANY = 2276
PG_TYPE_ANYARRAY = 2277
PG_TYPE_VOID = 2278
PG_TYPE_TRIGGER = 2279
PG_TYPE_EVTTRIGGER = 3838
PG_TYPE_LANGUAGE_HANDLER = 2280
PG_TYPE_INTERNAL = 2281
PG_TYPE_OPAQUE = 2282
PG_TYPE_ANYELEMENT = 2283
PG_TYPE_ANYNONARRAY = 2776
PG_TYPE_ANYENUM = 3500
PG_TYPE_FDW_HANDLER = 3115
PG_TYPE_ANYRANGE = 3831

class TZ(datetime.tzinfo):
    def __init__(self, offset='+00'):
        self.offset = offset
        hours = int(self.offset[:3])
        if len(offset) > 3:
            minutes = int(self.offset[4:6])
        else:
            minutes = 0
        if len(offset) > 6:
            seconds = int(self.offset[7:])
        else:
            seconds = 0
        self.delta = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)

    def utcoffset(self, dt):
        return self.delta

    def dst(self, dt):
        return self.delta

    def tzname(self, dt):
        return u"UTC" + self.offset

def _decode_column(data, oid, encoding, tzinfo, use_tzinfo):
    if data is None:
        return data
    data = data.decode(encoding)
    if oid in (PG_TYPE_BOOL,):
        return data == 't'
    elif oid in (PG_TYPE_INT2, PG_TYPE_INT4, PG_TYPE_INT8, PG_TYPE_OID,):
        return int(data)
    elif oid in (PG_TYPE_FLOAT4, PG_TYPE_FLOAT8):
        return float(data)
    elif oid in (PG_TYPE_NUMERIC, ):
        return decimal.Decimal(data)
    elif oid in (PG_TYPE_DATE, ):
        dt = datetime.datetime.strptime(data, '%Y-%m-%d')
        return datetime.date(dt.year, dt.month, dt.day)
    elif oid in (PG_TYPE_TIME, ):
        if len(data) == 8:
            dt = datetime.datetime.strptime(data, '%H:%M:%S')
        else:
            dt = datetime.datetime.strptime(data, '%H:%M:%S.%f')
        dt = datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond)
        if tzinfo:
            dt = dt.replace(tzinfo=tzinfo)
        return dt
    elif oid in (PG_TYPE_TIMESTAMP, ):
        if len(data) == 19:
            dt = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
        else:
            dt = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')
        if tzinfo:
            dt = dt.replace(tzinfo=tzinfo)
        return dt
    elif oid in (PG_TYPE_TIMETZ, ):
        n = data.rfind('+')
        if n == -1:
            n = data.rfind('-')
        s = data[:n]
        offset = int(data[n:])
        if len(s) == 8:
            dt = datetime.datetime.strptime(s, '%H:%M:%S')
        else:
            dt = datetime.datetime.strptime(s, '%H:%M:%S.%f')
        if use_tzinfo:
            dt = dt.replace(tzinfo=TZ(data[n:]))
        return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=dt.tzinfo)
    elif oid in (PG_TYPE_TIMESTAMPTZ, ):
        n = data.rfind('+')
        if n == -1:
            n = data.rfind('-')
        s = data[:n]
        if len(s) == 19:
            dt = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        else:
            dt = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
        if use_tzinfo:
            dt = dt.replace(tzinfo=TZ(data[n:]))
        return dt
    elif oid in (PG_TYPE_INTERVAL, ):
        if data == '1 day':
            return datetime.timedelta(days=1)
        dt = data.split('days')
        if len(dt) < 2:
            days = 0
            hours, minites, seconds = dt[0].split(':')
            seconds, microseconds = seconds.split('.')
            if not microseconds:
                microseconds = 0
        else:
            days = int(dt[0])
            if dt[1]:
                hours, minites, seconds = dt[1].split(':')
                seconds, microseconds = seconds.split('.')
                if not microseconds:
                    microseconds = 0
            else:
                hours = minites = seconds = microseconds = 0

        hours = int(hours)
        minites = int(minites)
        seconds = int(seconds)
        microseconds = int(microseconds)
        return datetime.timedelta(microseconds=microseconds,
            seconds=seconds, minutes=minites, hours=hours, days=days)
    elif oid in (PG_TYPE_BYTEA, ):
        assert data[:2] == u'\\x'
        hex_str = data[2:]
        ia = [int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2)]
        return b''.join([chr(c) for c in ia]) if PY2 else bytes(ia)
    elif oid in (PG_TYPE_TEXT, PG_TYPE_BPCHAR, PG_TYPE_VARCHAR, PG_TYPE_NAME, PG_TYPE_JSON):
        return data
    elif oid in (PG_TYPE_UUID, ):
        return uuid.UUID(data)
    elif oid in (PG_TYPE_UNKNOWN, PG_TYPE_PGNODETREE, PG_TYPE_TSVECTOR, PG_TYPE_INET):
        if DEBUG: DEBUG_OUTPUT('NO DECODE type:%d' % (oid, ))
        return data
    elif oid in (PG_TYPE_INT2ARRAY, PG_TYPE_INT4ARRAY):
        return [int(i) for i in data[1:-1].split(',')]
    elif oid in (PG_TYPE_NAMEARRAY, PG_TYPE_TEXTARRAY):
        return [s for s in data[1:-1].split(',')]
    elif oid in (PG_TYPE_FLOAT4ARRAY, ):
        return [float(f) for f in data[1:-1].split(',')]
    elif oid in (PG_TYPE_INT2VECTOR, ):
        return [int(i) for i in data.split(' ')]
    else:
        if DEBUG:
            raise ValueError('Unknown oid=' + str(oid) + ":" + data)
    return data

# ----------------------------------------------------------------------------

def _bytes_to_bint(b):     # Read as big endian
    if PY2:
        r = 0
        for n in b:
            r = r * 256 + ord(n)
        return r
    else:
        return int.from_bytes(b, byteorder='big')

def _bint_to_bytes(val):    # Convert int value to big endian 4 bytes.
    if PY2:
        return chr((val >> 24) & 0xff) + chr((val >> 16) & 0xff) + chr((val >> 8) & 0xff) + chr(val & 0xff)
    else:
        return val.to_bytes(4, byteorder='big')

Date = datetime.date
Time = datetime.time
TimeDelta = datetime.timedelta
Timestamp = datetime.datetime
def Binary(b):
    return bytearray(b)

class DBAPITypeObject:
    def __init__(self,*values):
        self.values = values
    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1
STRING = DBAPITypeObject(str)
BINARY = DBAPITypeObject(bytes)
NUMBER = DBAPITypeObject(int, decimal.Decimal)
DATETIME = DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
DATE = DBAPITypeObject(datetime.date)
TIME = DBAPITypeObject(datetime.time)
ROWID = DBAPITypeObject()

class Error(Exception):
    def __init__(self, *args):
        if len(args) > 0:
            self.message = args[0]
        else:
            self.message = b'Database Error'
        super(Error, self).__init__(*args)
    def __str__(self):
        return self.message
    def __repr__(self):
        return self.message

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DisconnectByPeer(Warning):
    pass

class InternalError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'InternalError')

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'NotSupportedError')

class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.description = []
        self._rows = collections.deque()
        self._rowcount = 0
        self.arraysize = 1
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def callproc(self, procname, args=()):
        raise NotSupportedError()

    def nextset(self, procname, args=()):
        raise NotSupportedError()

    def setinputsizes(sizes):
        pass

    def setoutputsize(size,column=None):
        pass

    def execute(self, query, args=()):
        if not self.connection or not self.connection.is_connect():
            raise ProgrammingError(u"08003:Lost connection")
        self.description = []
        self._rows.clear()
        self.args = args
        if args:
            escaped_args = tuple(
                self.connection.escape_parameter(arg).replace(u'%', u'%%') for arg in args
            )
            query = query.replace(u'%', u'%%').replace(u'%%s', u'%s')
            query = query % escaped_args
            query = query.replace(u'%%', u'%')
        self.query = query
        self.connection.execute(query, self)

    def executemany(self, query, seq_of_params):
        rowcount = 0
        for params in seq_of_params:
            self.execute(query, params)
            rowcount += self._rowcount
        self._rowcount = rowcount

    def fetchone(self):
        if not self.connection or not self.connection.is_connect():
            raise OperationalError(u"08003:Lost connection")
        if len(self._rows):
            return self._rows.popleft()
        return None

    def fetchmany(self, size=1):
        rs = []
        for i in range(size):
            r = self.fetchone()
            if not r:
                break
            rs.append(r)
        return rs

    def fetchall(self):
        r = list(self._rows)
        self._rows.clear()
        return r

    def close(self):
        self.connection = None

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def closed(self):
        return self.connection is None or not self.connection.is_connect()


    def __iter__(self):
        return self

    def __next__(self):
        r = self.fetchone()
        if not r:
            raise StopIteration()
        return r

    def next(self):
        return self.__next__()

class Connection(object):
    def __init__(self, user, password, database, host, port, tzinfo, timeout, use_ssl):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.tzinfo = tzinfo
        self.timeout = timeout
        self.use_ssl = use_ssl
        self.encoding = 'UTF8'
        self.autocommit = False
        self._ready_for_query = b'I'
        self.use_tzinfo = True
        self._open()
        self.encoders = {}

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def _send_message(self, message, data):
        self._write(b''.join([
                message,
                _bint_to_bytes(len(data) + 4),
                data,
                b'H\x00\x00\x00\x04',   # Flush
            ])
        )

    def _process_messages(self, obj):
        errobj = None
        while True:
            code = ord(self._read(1))
            ln = _bytes_to_bint(self._read(4)) - 4
            data = self._read(ln)
            if DEBUG:
                DEBUG_OUTPUT("%d\t%i\t%s" % (code, ln, binascii.b2a_hex(data)))
            if code == 90:  # ReadyForQuery('Z')
                self._ready_for_query = data
                break
            elif code == 82:    # Authenticaton('R')
                auth_method = _bytes_to_bint(data[:4])
                if auth_method == 0:      # trust
                    pass
                elif auth_method == 5:    # md5
                    import hashlib
                    salt = data[4:]
                    hash1 = hashlib.md5(self.password.encode('ascii') + self.user.encode("ascii")).hexdigest().encode("ascii")
                    hash2 = hashlib.md5(hash1+salt).hexdigest().encode("ascii")
                    self._send_message(b'p', b''.join([b'md5',hash2,'\x00']))
                else:
                    errobj = InterfaceError("Authentication method %d not supported." % (auth_method,))
            elif code == 83:    # ParamterStatus('S')
                k, v, _ = data.split(b'\x00')
                if k == b'server_encoding':
                    self.encoding = v.decode('ascii')
            elif code == 75:    # BackendKeyData('K')
                pass
            elif code == 67:    # CommandComplte('C')
                if not obj:
                    continue
                command = data[:-1].decode('ascii')
                if command == 'SHOW':
                    obj._rowcount = 1
                else:
                    for k in ('SELECT', 'UPDATE', 'DELETE', 'INSERT'):
                        if command[:len(k)] == k:
                            obj._rowcount = int(command.split(' ')[-1])
                            break
            elif code == 84:    # RowDescription('T')
                if not obj:
                    continue
                count = _bytes_to_bint(data[0:2])
                obj.description = [None] * count
                n = 2
                idx = 0
                for i in range(count):
                    name = data[n:n+data[n:].find(b'\x00')]
                    n += len(name) + 1
                    try:
                        name = name.decode(self.encoding)
                    except UnicodeDecodeError:
                        pass
                    type_code = _bytes_to_bint(data[n+6:n+10])
                    if type_code == PG_TYPE_VARCHAR:
                        size = _bytes_to_bint(data[n+12:n+16]) - 4
                        precision = -1
                        scale = -1
                    elif type_code == PG_TYPE_NUMERIC:
                        size = _bytes_to_bint(data[n+10:n+12])
                        precision = _bytes_to_bint(data[n+12:n+14])
                        scale = precision - _bytes_to_bint(data[n+14:n+16])
                    else:
                        size = _bytes_to_bint(data[n+10:n+12])
                        precision = -1
                        scale = -1
#                        table_oid = _bytes_to_bint(data[n:n+4])
#                        table_pos = _bytes_to_bint(data[n+4:n+6])
#                        size = _bytes_to_bint(data[n+10:n+12])
#                        modifier = _bytes_to_bint(data[n+12:n+16])
#                        format = _bytes_to_bint(data[n+16:n+18]),
                    field = (name, type_code, None, size, precision, scale, None)
                    n += 18
                    obj.description[idx] = field
                    idx += 1
            elif code == 68:    # DataRow('D')
                if not obj:
                    continue
                n = 2
                row = []
                while n < len(data):
                    if data[n:n+4] == b'\xff\xff\xff\xff':
                        row.append(None)
                        n += 4
                    else:
                        ln = _bytes_to_bint(data[n:n+4])
                        n += 4
                        row.append(data[n:n+ln])
                        n += ln
                for i in range(len(row)):
                    row[i] = _decode_column(row[i], obj.description[i][1], self.encoding, self.tzinfo, self.use_tzinfo)
                obj._rows.append(tuple(row))
            elif code == 78:    # NoticeResponse('N')
                pass
            elif code == 69 and not errobj: # ErrorResponse('E')
                err = data.split(b'\x00')
                # http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
                errcode = err[1][1:]
                message = errcode + b':' + err[2][1:]
                if not PY2:
                    message = message.decode(self.encoding)
                if errcode[:2] == b'23':
                    errobj = IntegrityError(message)
                else:
                    errobj = DatabaseError(message)
            elif code == 72:    # CopyOutputResponse('H')
                pass
            elif code == 100:   # CopyData('d')
                obj.write(data)
            elif code == 99:    # CopyDataDone('c')
                pass
            elif code == 71:    # CopyInResponse('G')
                while True:
                    buf = obj.read(8192)
                    if not buf:
                        break
                    # send CopyData
                    self._write(b'd' + _bint_to_bytes(len(buf) + 4))
                    self._write(buf)
                # send CopyDone and Sync
                self._write(b'c\x00\x00\x00\x04S\x00\x00\x00\x04')
            else:
                pass
        return errobj

    def process_messages(self, obj):
        err = self._process_messages(obj)
        if err:
            raise err

    def _read(self, ln):
        if not self.sock:
            raise OperationalError(u"08003:Lost connection")
        r = b''
        while len(r) < ln:
            b = self.sock.recv(ln-len(r))
            if not b:
                raise OperationalError(u"08003:Can't recv packets")
            r += b
        return r

    def _write(self, b):
        if sys.platform == 'cli':
            # A workaround for IronPython 2.7.5b2 problem
            b = str(b)
        if not self.sock:
            raise OperationalError(u"08003:Lost connection")
        n = 0
        while (n < len(b)):
            n += self.sock.send(b[n:])

    def _open(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        if DEBUG: DEBUG_OUTPUT("socket %s:%d" % (self.host, self.port))
        if self.use_ssl:
            import ssl
            self._write(_bint_to_bytes(8))
            self._write(_bint_to_bytes(80877103))    # SSL request
            if self._read(1) == b'S':
                self.sock = ssl.wrap_socket(self.sock)
            else:
                raise InterfaceError("Server refuses SSL")
        if self.timeout is not None:
            self.sock.settimeout(float(self.timeout))
        # protocol version 3.0
        v = _bint_to_bytes(196608)
        v += b'user\x00' + self.user.encode('ascii') + b'\x00'
        if self.database:
            v += b'database\x00' + self.database.encode('ascii') + b'\x00'
        v += b'\x00'

        self._write(_bint_to_bytes(len(v) + 4) + v)
        self.process_messages(None)

    def escape_parameter(self, v):
        t = type(v)
        func = self.encoders.get(t)
        if func:
            return func(self, v)
        if v is None:
            return 'NULL'
        elif (PY2 and t == unicode) or (not PY2 and t == str):  # string
            return u"'" + v.replace(u"'", u"''") + u"'"
        elif PY2 and t == str:    # PY2 str
            v = ''.join(['\\%03o' % (ord(c), ) if ord(c) < 32 or ord(c) > 127 else c for c in v])
            return "'" + v.replace("'", "''") + "'"
        elif t == bytearray or t == bytes:        # binary
            return "'" + ''.join(['\\%03o' % (c, ) for c in v]) + "'::bytea"
        elif t == bool:
            return u"TRUE" if v else u"FALSE"
        elif t == time.struct_time:
            return u'%04d-%02d-%02d %02d:%02d:%02d' % (
                v.tm_year, v.tm_mon, v.tm_mday, v.tm_hour, v.tm_min, v.tm_sec)
        elif t == datetime.datetime:
            return "'" + str(v) + "'"
        elif t == datetime.date:
            return "'" + str(v) + "'"
        elif t == datetime.timedelta:
            if v.seconds:
                if v.microseconds:
                    return u"interval '%d.%06d second'" % (v.days * 86400 + v.seconds, v.microseconds)
                else:
                    return u"interval '%d second'" % (v.days * 86400 + v.seconds, )
            else:
                return u"interval '%d day'" % (v.days, )
        elif t == int or t == float or (PY2 and t == long):
            return str(v)
        elif t == decimal.Decimal:
            return "'" + str(v) + "'"
        elif t == list or t == tuple:
            return u'ARRAY[' + u','.join([self.escape_parameter(e) for e in v]) + u']'
        else:
            return "'" + str(v) + "'"

    @property
    def is_dirty(self):
        return self._ready_for_query in b'TE'

    def is_connect(self):
        return bool(self.sock)

    def cursor(self):
        return Cursor(self)

    def _execute(self, query, obj):
        if DEBUG: DEBUG_OUTPUT('Connection::_execute()\t%s' % (query, ))
        self._send_message(b'Q', query.encode(self.encoding) + b'\x00')
        self.process_messages(obj)
        if self.autocommit:
            self.commit()

    def execute(self, query, obj=None):
        if self._ready_for_query != b'T':
            self.begin()
        self._execute(query, obj)

    def set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def begin(self):
        if DEBUG: DEBUG_OUTPUT('BEGIN')
        if self._ready_for_query == b'E':
            self.rollback()
        self._send_message(b'Q', b"BEGIN\x00")
        self._process_messages(None)

    def commit(self):
        if DEBUG: DEBUG_OUTPUT('COMMIT')
        if self.sock:
            self._send_message(b'Q', b"COMMIT\x00")
            self.process_messages(None)

    def rollback(self):
        if self.sock:
            if DEBUG: DEBUG_OUTPUT('ROLLBACK')
            self._send_message(b'Q', b"ROLLBACK\x00")
        self.process_messages(None)

    def reopen(self):
        self.close()
        self._open()

    def close(self):
        if DEBUG: DEBUG_OUTPUT('Connection::close()')
        if self.sock:
            # send Terminate
            self._write(b'X\x00\x00\x00\x04')
            self.sock.close()
            self.sock = None

def connect(host, user, password='', database=None, port=5432, tzinfo=None, timeout=None, use_ssl=False):
    return Connection(user, password, database, host, port, tzinfo, timeout, use_ssl)

