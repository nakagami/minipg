##############################################################################
#The MIT License (MIT)
#
#Copyright (c) 2014-2021 Hajime Nakagami
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
import string
import decimal
import datetime
import time
import uuid
import collections
import binascii
import re
import random
import hashlib
import base64
import hmac
import enum
from argparse import ArgumentParser

from . import (
    InterfaceError,
    DatabaseError,
    InternalError,
    OperationalError,
    ProgrammingError,
    IntegrityError,
    DataError,
    NotSupportedError,
)


VERSION = (0, 8, 0)
__version__ = '%s.%s.%s' % VERSION
apilevel = '2.0'
threadsafety = 1
paramstyle = 'format'

DEBUG = False


def DEBUG_OUTPUT(s):
    if DEBUG:
        print(s, end=' \n', file=sys.stderr)

class PgTzinfo(datetime.tzinfo):
    def __init__(self, name=None, cur=None):
        if name is None or cur is None:
            self._offset = datetime.timedelta(0)
            self.zone = 'UTC'
        else:
            cur.execute('SELECT utc_offset FROM pg_timezone_names WHERE name=%s', name)
            self._offset = cur.fetchone()[0]
            self.zone = name

    def utcoffset(self, dt):
        return self._offset

    def tzname(self, dt):
        return self.zone

    def dst(self, dt):
        return datetime.timedelta(0)


#-----------------------------------------------------------------------------
# http://www.postgresql.org/docs/9.6/static/protocol.html
# http://www.postgresql.org/docs/9.6/static/protocol-message-formats.html

# https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h
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
PG_TYPE_BOOLARRAY = 1000
PG_TYPE_NAMEARRAY = 1003
PG_TYPE_INT2ARRAY = 1005
PG_TYPE_INT4ARRAY = 1007
PG_TYPE_TEXTARRAY = 1009
PG_TYPE_VARCHARARRAY = 1015
PG_TYPE_FLOAT4ARRAY = 1021
PG_TYPE_ARRAYOID = 1028
PG_TYPE_ACLITEM = 1033
PG_TYPE_BPCHAR = 1042
PG_TYPE_VARCHAR = 1043
PG_TYPE_DATE = 1082
PG_TYPE_TIME = 1083
PG_TYPE_TIMESTAMP = 1114
PG_TYPE_TIMESTAMPTZ = 1184
PG_TYPE_INTERVAL = 1186
PG_TYPE_CSTRINGARRAY = 1263
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
PG_TYPE_JSONBOID = 3802
PG_TYPE_ANYRANGE = 3831


def _bytes_to_bint(b):     # Read as big endian
    return int.from_bytes(b, byteorder='big')


def _bint_to_bytes(val):    # Convert int value to big endian 4 bytes.
    return val.to_bytes(4, byteorder='big')

Date = datetime.date
Time = datetime.time
TimeDelta = datetime.timedelta
Timestamp = datetime.datetime


def Binary(b):
    return bytearray(b)


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
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

Description = collections.namedtuple(
    'Description',
    ('name', 'type_code', 'display_size', 'internal_size', 'precision', 'scale', 'null_ok')
)

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

    def nextset(self, procname, args=()):
        raise NotSupportedError()

    def setinputsizes(sizes):
        pass

    def setoutputsize(size, column=None):
        pass

    def execute(self, query, args=None):
        if not self.connection or not self.connection.is_connect():
            raise InterfaceError("Lost connection", "08003")
        self.description = []
        self._rows.clear()
        self.args = args
        if args is not None:
            if isinstance(args, (tuple, list)):
                escaped_args = tuple(
                    [self.connection.escape_parameter(arg) for arg in args]
                )
            elif isinstance(args, dict):
                escaped_args = {
                    k: self.connection.escape_parameter(v) for (k, v) in args.items()
                }
            else:
                escaped_args = self.connection.escape_parameter(args)
            query = query % escaped_args
        self.query = query
        self.connection.execute(self.query, self)

    def callproc(self, proc_name, args=None):
        escaped_args = []
        if args is not None:
            if isinstance(args, (tuple, list)):
                escaped_args = tuple(
                    [self.connection.escape_parameter(arg) for arg in args]
                )
            elif isinstance(args, dict):
                escaped_args = {
                    k: self.connection.escape_parameter(v) for (k, v) in args.items()
                }
            else:
                escaped_args = self.connection.escape_parameter(args)
        self.query = 'select * from ' + proc_name + '(' +  ','.join(escaped_args) + ')'
        self.connection.execute(self.query, self)

    def executemany(self, query, seq_of_params):
        rowcount = 0
        for params in seq_of_params:
            self.execute(query, params)
            rowcount += self._rowcount
        self._rowcount = rowcount

    def fetchone(self):
        if not self.connection or not self.connection.is_connect():
            raise InterfaceError("Lost connection", "08003")
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
    def __init__(self, user, password, database, host, port, timeout, use_ssl):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.timeout = timeout
        self.use_ssl = use_ssl
        self.encoding = 'UTF8'
        self.autocommit = False
        self.server_version = ''
        self._trans_status = b'I'
        self.encoders = {}
        self.tz_name = None
        self.tzinfo = None
        self._open()

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def _send_data(self, message, data):
        DEBUG_OUTPUT('<- {}:{}'.format(message, data))
        self._write(b''.join([message, _bint_to_bytes(len(data) + 4), data]))

    def _send_message(self, message, data):
        DEBUG_OUTPUT('<- {}:{}'.format(message, data))
        self._write(b''.join([message, _bint_to_bytes(len(data) + 4), data, b'H\x00\x00\x00\x04']))

    def _decode_column(self, data, oid):
        def _trim_timezone_offset(data):
            n = data.rfind('+')
            if n == -1:
                n = data.rfind('-')
            return data[:n]

        def _parse_point(data):
            x, y = data[1:-1].split(',')
            return (float(x), float(y))

        if data is None:
            return data
        data = data.decode(self.encoding)
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
            return dt
        elif oid in (PG_TYPE_TIMESTAMP, ):
            if len(data) == 19:
                dt = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
            else:
                dt = datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')
            return dt
        elif oid in (PG_TYPE_TIMETZ, ):
            s = _trim_timezone_offset(data)
            if len(s) == 8:
                t = datetime.datetime.strptime(s, '%H:%M:%S')
            else:
                t = datetime.datetime.strptime(s, '%H:%M:%S.%f')
            t = t.replace(tzinfo=self.tzinfo)
            return t
        elif oid in (PG_TYPE_TIMESTAMPTZ, ):
            s = _trim_timezone_offset(data)
            if len(s) == 19:
                dt = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
            else:
                dt = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
            dt = dt.replace(tzinfo=self.tzinfo)
            return dt
        elif oid in (PG_TYPE_INTERVAL, ):
            dt = re.split('days?', data)
            if len(dt) < 2:
                days = 0
                t = dt[0]
            else:
                days = dt[0]
                t = dt[1]
            if t:
                hours, minites, seconds = t.split(':')
                if seconds.find('.') != -1:
                    seconds, microseconds = seconds.split('.')
                    microseconds += "0" * (6 - len(microseconds))
                else:
                    microseconds = 0
            else:
                hours = minites = seconds = microseconds = 0
            return datetime.timedelta(
                microseconds=int(microseconds),
                seconds=int(seconds),
                minutes=int(minites),
                hours=int(hours),
                days=int(days),
            )
        elif oid in (PG_TYPE_BYTEA, ):
            assert data[:2] == u'\\x'
            hex_str = data[2:]
            ia = [int(hex_str[i:i+2], 16) for i in range(0, len(hex_str), 2)]
            return bytes(ia)
        elif oid in (PG_TYPE_CHAR, PG_TYPE_TEXT, PG_TYPE_BPCHAR, PG_TYPE_VARCHAR, PG_TYPE_NAME, PG_TYPE_JSONBOID, PG_TYPE_XML):
            return data
        elif oid in (PG_TYPE_UUID, ):
            return uuid.UUID(data)
        elif oid in (PG_TYPE_UNKNOWN, PG_TYPE_PGNODETREE, PG_TYPE_TSVECTOR, PG_TYPE_INET):
            DEBUG_OUTPUT('NO DECODE type:%d' % (oid, ))
            return data
        elif oid in (PG_TYPE_BOOLARRAY, ):
            return [b == 't' for b in data[1:-1].split(',')]
        elif oid in (PG_TYPE_INT2ARRAY, PG_TYPE_INT4ARRAY):
            return [int(i) for i in data[1:-1].split(',')]
        elif oid in (PG_TYPE_NAMEARRAY, PG_TYPE_TEXTARRAY, PG_TYPE_VARCHARARRAY):
            return [s for s in data[1:-1].split(',')]
        elif oid in (PG_TYPE_FLOAT4ARRAY, ):
            return [float(f) for f in data[1:-1].split(',')]
        elif oid in (PG_TYPE_INT2VECTOR, ):
            return [int(i) for i in data.split(' ')]
        elif oid in (PG_TYPE_POINT, ):
            return _parse_point(data)
        elif oid in (PG_TYPE_CIRCLE, ):
            p = data[1:data.find(')')+1]
            r = data[len(p)+2:-1]
            return (_parse_point(p), float(r))
        elif oid in (PG_TYPE_LSEG, PG_TYPE_PATH, PG_TYPE_BOX, PG_TYPE_POLYGON, PG_TYPE_LINE):
            return eval(data)
        elif oid in (PG_TYPE_VOID, ):
            return None
        else:
            if DEBUG:
                raise ValueError('Unknown oid=' + str(oid) + ":" + data)
        return data

    def _process_messages(self, obj):
        errobj = None
        while True:
            try:
                code = ord(self._read(1))
            except OperationalError:
                # something error occured
                break
            ln = _bytes_to_bint(self._read(4)) - 4
            data = self._read(ln)
            if code == 90:
                self._trans_status = data
                DEBUG_OUTPUT("-> ReadyForQuery('Z'):{}".format(data))
                break
            elif code == 82:
                auth_method = _bytes_to_bint(data[:4])
                DEBUG_OUTPUT("-> Authentication('R'):{}".format(auth_method))
                if auth_method == 0:      # trust
                    pass
                elif auth_method == 5:    # md5
                    salt = data[4:]
                    hash1 = hashlib.md5(self.password.encode('ascii') + self.user.encode("ascii")).hexdigest().encode("ascii")
                    hash2 = hashlib.md5(hash1+salt).hexdigest().encode("ascii")
                    self._send_data(b'p', b''.join([b'md5', hash2, b'\x00']))

                    # accept
                    code = ord(self._read(1))
                    assert code == 82
                    ln = _bytes_to_bint(self._read(4)) - 4
                    data = self._read(ln)
                    assert _bytes_to_bint(data[:4]) == 0
                elif auth_method == 10:   # SASL
                    assert data[4:-2].decode('utf-8') == 'SCRAM-SHA-256'
                    printable = string.ascii_letters + string.digits + '+/'
                    client_nonce = ''.join(
                        printable[random.randrange(0, len(printable))]
                        for i in range(24)
                    )

                    # send client first message
                    first_message = 'n,,n=,r=' + client_nonce
                    self._send_data(b'p', b''.join([
                        b'SCRAM-SHA-256\x00',
                        _bint_to_bytes(len(first_message)),
                        first_message.encode('utf-8')
                    ]))

                    code = ord(self._read(1))
                    assert code == 82
                    ln = _bytes_to_bint(self._read(4)) - 4
                    data = self._read(ln)
                    _bytes_to_bint(data[:4]) == 11      # SCRAM first

                    server = {
                        kv[0]: kv[2:]
                        for kv in data[4:].decode('utf-8').split(',')
                    }
                    # r: server nonce
                    # s: servre salt
                    # i: iteration count
                    assert server['r'][:len(client_nonce)] == client_nonce

                    # send client final message
                    salted_pass = hashlib.pbkdf2_hmac(
                        'sha256',
                        self.password.encode('utf-8'),
                        base64.standard_b64decode(server['s']),
                        int(server['i']),
                    )

                    client_key = hmac.HMAC(
                        salted_pass, b"Client Key", hashlib.sha256
                    ).digest()

                    client_first_message_bare = "n=,r=" + client_nonce
                    server_first_message = "r=%s,s=%s,i=%s" % (server['r'], server['s'], server['i'])
                    client_final_message_without_proof = "c=biws,r=" + server['r']
                    auth_msg = ','.join([
                        client_first_message_bare,
                        server_first_message,
                        client_final_message_without_proof
                    ])

                    client_sig = hmac.HMAC(
                        hashlib.sha256(client_key).digest(),
                        auth_msg.encode('utf-8'),
                        hashlib.sha256
                    ).digest()

                    proof = base64.standard_b64encode(
                        b"".join([bytes([x ^ y]) for x, y in zip(client_key, client_sig)])
                    )
                    self._send_data(b'p',
                        (client_final_message_without_proof + ",p=").encode('utf-8') + proof
                    )

                    code = ord(self._read(1))
                    assert code == 82
                    ln = _bytes_to_bint(self._read(4)) - 4
                    data = self._read(ln)
                    _bytes_to_bint(data[:4]) == 12      # SCRAM final

                    # accept
                    code = ord(self._read(1))
                    assert code == 82
                    ln = _bytes_to_bint(self._read(4)) - 4
                    data = self._read(ln)
                    assert _bytes_to_bint(data[:4]) == 0
                else:
                    errobj = InterfaceError("Authentication method %d not supported." % (auth_method,))
            elif code == 83:
                k, v, _ = data.split(b'\x00')
                DEBUG_OUTPUT("-> ParameterStatus('S'):{}:{}".format(k, v))
                if k == b'server_encoding':
                    self.encoding = v.decode('ascii')
                elif k == b'server_version':
                    version = v.decode('ascii').split('(')[0].split('.')
                    self.server_version  = int(version[0]) * 10000
                    if len(version) > 0:
                        try:
                            self.server_version += int(version[1]) * 100
                        except:
                            pass
                    if len(version) > 1:
                        try:
                            self.server_version += int(version[2])
                        except:
                            pass
                elif k == b'TimeZone':
                    self.tz_name = v.decode('ascii')
                    self.tzinfo = None
            elif code == 75:
                DEBUG_OUTPUT("-> BackendKeyData('K')")
                pass
            elif code == 67:
                if not obj:
                    DEBUG_OUTPUT("-> CommandComplete('C')")
                    continue
                command = data[:-1].decode('ascii')
                DEBUG_OUTPUT("-> CommandComplete('C'):{}".format(command))
                if command == 'SHOW':
                    obj._rowcount = 1
                else:
                    for k in ('SELECT', 'UPDATE', 'DELETE', 'INSERT'):
                        if command[:len(k)] == k:
                            obj._rowcount = int(command.split(' ')[-1])
                            break
            elif code == 84:
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
                    field = Description(name, type_code, None, size, precision, scale, None)
                    n += 18
                    obj.description[idx] = field
                    idx += 1
                DEBUG_OUTPUT("-> RowDescription('T'):{}".format(obj.description))
            elif code == 68:
                if not obj:
                    DEBUG_OUTPUT("-> DataRow('D')")
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
                    row[i] = self._decode_column(row[i], obj.description[i][1])
                obj._rows.append(tuple(row))
                DEBUG_OUTPUT("-> DataRow('D'):{}".format(tuple(row)))
            elif code == 78:
                DEBUG_OUTPUT("-> NoticeResponse('N')")
                pass
            elif code == 69 and not errobj:
                err = data.split(b'\x00')
                # http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
                errcode = err[2][1:].decode('utf-8')
                message = "{}:{}".format(self.query, err[3][1:].decode(self.encoding))
                DEBUG_OUTPUT("-> ErrorResponse('E'):{}:{}".format(errcode, message))

                if errcode[:2] == '0A':
                    errobj = NotSupportedError(message, errcode)
                elif errcode[:2] in ('20', '21'):
                    errobj = ProgrammingError(message, errcode)
                elif errcode[:2] in ('22', ):
                    errobj = DataError(message, errcode)
                elif errcode[:2] == '23':
                    errobj = IntegrityError(message, errcode)
                elif errcode[:2] in('24', '25'):
                    errobj = InternalError(message, errcode)
                elif errcode[:2] in('26', '27', '28'):
                    errobj = OperationalError(message, errcode)
                elif errcode[:2] in('2B', '2D', '2F'):
                    errobj = InternalError(message, errcode)
                elif errcode[:2] == '34':
                    errobj = OperationalError(message, errcode)
                elif errcode[:2] in ('38', '39', '3B'):
                    errobj = InternalError(message, errcode)
                elif errcode[:2] in ('3D', '3F'):
                    errobj = ProgrammingError(message, errcode)
                elif errcode[:2] in ('40', '42', '44'):
                    errobj = ProgrammingError(message, errcode)
                elif errcode[:1] == '5':
                    errobj = OperationalError(message, errcode)
                elif errcode[:1] in 'F':
                    errobj = InternalError(message, errcode)
                elif errcode[:1] in 'H':
                    errobj = OperationalError(message, errcode)
                elif errcode[:1] in ('P', 'X'):
                    errobj = InternalError(message, errcode)
                else:
                    errobj = DatabaseError(message, errcode)
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
                DEBUG_OUTPUT("-> Unknown({}):{}{}".format(code, ln, binascii.b2a_hex(data)))
                pass
        return errobj

    def process_messages(self, obj):
        err = self._process_messages(obj)
        if err:
            raise err

    def _read(self, ln):
        if not self.sock:
            raise InterfaceError("Lost connection", "08003")
        r = b''
        while len(r) < ln:
            b = self.sock.recv(ln-len(r))
            if not b:
                raise InterfaceError("Can't recv packets", "08003")
            r += b
        return r

    def _write(self, b):
        if not self.sock:
            raise InterfaceError("Lost connection", "08003")
        n = 0
        while (n < len(b)):
            n += self.sock.send(b[n:])

    def _open(self):
        self.sock = socket.create_connection((self.host, self.port), self.timeout)
        DEBUG_OUTPUT("Connection._open() socket %s:%d" % (self.host, self.port))
        if self.use_ssl:
            import ssl
            self._write(_bint_to_bytes(8))
            self._write(_bint_to_bytes(80877103))    # SSL request
            if self._read(1) == b'S':
                self.sock = ssl.wrap_socket(self.sock)
            else:
                raise InterfaceError("Server refuses SSL")
        # protocol version 3.0
        v = b'\x00\x03\x00\x00'
        v += b'user\x00' + self.user.encode('ascii') + b'\x00'
        if self.database:
            v += b'database\x00' + self.database.encode('ascii') + b'\x00'
        v += b'\x00'

        self._write(_bint_to_bytes(len(v) + 4) + v)
        self.process_messages(None)

        self._begin()

        if self.tz_name and self.tzinfo is None:
            self.set_timezone(self.tz_name)

    def escape_parameter(self, v):
        if isinstance(v, enum.Enum):
            v = v.value
        t = type(v)
        func = self.encoders.get(t)
        if func:
            return func(self, v)
        if v is None:
            return 'NULL'
        elif t == str:
            return u"'" + v.replace(u"'", u"''") + u"'"
        elif t == bytearray or t == bytes:        # binary
            return "'" + ''.join(['\\%03o' % (c, ) for c in v]) + "'::bytea"
        elif t == bool:
            return u"TRUE" if v else u"FALSE"
        elif t == time.struct_time:
            return u'%04d-%02d-%02d %02d:%02d:%02d' % (
                v.tm_year, v.tm_mon, v.tm_mday, v.tm_hour, v.tm_min, v.tm_sec)
        elif t == datetime.datetime:
            if v.tzinfo:
                return "timestamp with time zone '" + v.isoformat() + "'"
            else:
                return "timestamp '" + v.isoformat() + "'"
        elif t == datetime.date:
            return "date '" + str(v) + "'"
        elif t == datetime.timedelta:
            return u"interval '" + str(v) + "'"
        elif t == int or t == float:
            return str(v)
        elif t == decimal.Decimal:
            return "decimal '" + str(v) + "'"
        elif t == list or t == tuple:
            return u'ARRAY[' + u','.join([self.escape_parameter(e) for e in v]) + u']'
        else:
            return "'" + str(v) + "'"

    def is_connect(self):
        return bool(self.sock)

    def cursor(self, factory=Cursor):
        return factory(self)

    def execute(self, query, obj=None):
        self.query = query
        self._send_message(b'Q', query.encode(self.encoding) + b'\x00')
        self.process_messages(obj)
        if self.autocommit:
            self.commit()

    def get_parameter_status(self, s):
        with self.cursor() as cur:
            cur.execute('SHOW {}'.format(s))
            return cur.fetchone()[0]

    def set_timezone(self, timezone_name):
        self.tz_name = timezone_name
        with self.cursor() as cur:
            cur.execute("SET TIME ZONE %s",  [self.tz_name])
            try:
                import pytz
                self.tzinfo = pytz.timezone(self.tz_name)
            except ImportError:
                self.tzinfo = PgTzinfo(self.tz_name, cur)

    @property
    def isolation_level(self):
        return self.get_parameter_status('TRANSACTION ISOLATION LEVEL')

    def set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def _begin(self):
        self._send_message(b'Q', b"BEGIN\x00")
        self._process_messages(None)

    def begin(self):
        if DEBUG:
            DEBUG_OUTPUT('BEGIN')
        self._begin()

    def commit(self):
        if DEBUG:
            DEBUG_OUTPUT('COMMIT')
        if self.sock:
            self._send_message(b'Q', b"COMMIT\x00")
            self.process_messages(None)
            self._begin()

    def _rollback(self):
        self._send_message(b'Q', b"ROLLBACK\x00")
        self._process_messages(None)

    def rollback(self):
        if DEBUG:
            DEBUG_OUTPUT('ROLLBACK')
        if self.sock:
            self._rollback()
            self._begin()

    def reopen(self):
        self.close()
        self._open()

    def close(self):
        if DEBUG:
            DEBUG_OUTPUT('Connection::close()')
        if self.sock:
            # send Terminate
            self._write(b'X\x00\x00\x00\x04')
            self.sock.close()
            self.sock = None


def connect(host, user, password='', database=None, port=None, timeout=None, use_ssl=False):
    return Connection(user, password, database, host, port if port else 5432, timeout, use_ssl)


def create_database(database, host, user, password='', port=None, use_ssl=False):
    with connect(host, user, password, None, port, None, use_ssl) as conn:
        conn._rollback()
        conn._send_message(b'Q', 'CREATE DATABASE {}'.format(database).encode('utf-8') + b'\x00')
        conn.process_messages(None)


def drop_database(database, host, user, password='', port=None, use_ssl=False):
    with connect(host, user, password, None, port, None, use_ssl) as conn:
        conn._rollback()
        conn._send_message(b'Q', 'DROP DATABASE {}'.format(database).encode('utf-8') + b'\x00')
        conn.process_messages(None)
