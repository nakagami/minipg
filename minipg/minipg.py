##############################################################################
#The MIT License (MIT)
#
#Copyright (c) 2014 Hajime Nakagami
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
import struct
import datetime

PY2 = sys.version_info[0] == 2

VERSION = (0, 0, 1)
__version__ = '%s.%s.%s' % VERSION
apilevel = '2.0'
threadsafety = 1
paramstyle = 'format'

DEBUG = True

# Format Codes
FC_BINARY = 1
FC_TEXT = 0

# http://www.postgresql.org/docs/9.3/static/protocol-message-formats.html
PG_B_AUTHENTICATION = b'R'
PG_B_BACKEND_KEY_DATA = b'K'
PG_F_BIND = b'B'
PG_B_BIND_COMPLETE = b'2'
PG_F_CLOSE = b'C'
PG_B_CLOSE_COMPLETE = b'3'
PG_B_COMMAND_COMPLETE = b'C'
PG_COPY_DATA = b'd'
PG_COPY_DONE = b'c'
PG_F_COPY_FALL = b'f'
PG_B_COPY_IN_RESPONSE = b'G'
PG_B_COPY_OUT_RESPONSE = b'H'
PG_B_COPY_BOTH_RESPONSE = b'W'
PG_B_DATA_ROW = b'D'
PG_F_DESCRIBE = b'D'
PG_B_EMPTY_QUERY_RESPONSE = b'I'
PG_B_ERROR_RESPONSE = b'E'
PG_F_EXECUTE = b'E'
PG_F_FLUSH = b'H'
PG_F_FUNCTION_CALL = b'F'
PG_B_FUNCTION_CALL_RESPONSE = b'V'
PG_B_NO_DATA = b'n'
PG_B_NOTICE_RESPNSE = b'N'
PG_B_NOTIFICATION_RESPONSE = b'A'
PG_B_PARAMETER_DESCRIPTION = b't'
PG_B_PARAMETER_STATUS = b'S'
PG_F_PARSE = b'P'
PG_B_PARSE_COMPLETE = b'1'
PG_F_PASSWORD_MESSAGE = b'p'
PG_B_PORTAL_SUSPEND = b's'
PG_F_QUERY = b'Q'
PG_B_READY_FOR_QUERY = b'Z'
PG_B_ROW_DESCRIPTION = b'T'
PG_F_SYNC = b'S'
PG_F_TERMINATE = b'X'

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

def _decode_bool(data, i, ln):
    return data[i] == b'\x01'

def _decode_int(data, i, ln):
    return _bytes_to_bint(data[i, i+ln])

def _decode_string(data, i, ln):
    return data[i, i+ln].decode('utf-8')

def _decode_bytes(data, i, ln):
    return data[i: i+ln]

def _decode_float4(data, i, ln):
    return struct.unpack("f", data[i:])[0]

def _decode_float8(data, i, ln):
    return struct.unpack("d", data[i:])[0]

def DEBUG_OUTPUT(*argv):
    if not DEBUG:
        return
    for s in argv:
        print(s, end=' ', file=sys.stderr)
    print(file=sys.stderr)

def _bytes_to_bint(b, u=False):     # Read as big endian
    if u:
        fmtmap = {1: 'B', 2: '>H', 4: '>L', 8: '>Q'}
    else:
        fmtmap = {1: 'b', 2: '>h', 4: '>l', 8: '>q'}
    fmt = fmtmap.get(len(b))
    if fmt is None:
        raise InternalError
    return struct.unpack(fmt, b)[0]

def _bint_to_bytes(val, nbytes):    # Convert int value to big endian bytes.
    v = abs(val)
    b = []
    for n in range(nbytes):
        b.append((v >> (8*(nbytes - n - 1)) & 0xff))
    if val < 0:
        for i in range(nbytes):
            b[i] = ~b[i] + 256
        b[-1] += 1
        for i in range(nbytes):
            if b[nbytes -i -1] == 256:
                b[nbytes -i -1] = 0
                b[nbytes -i -2] += 1
    return bs(b)

class Error(Exception):
    pass

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

class Connection(object):
    def __init__(self, user, password, database, host, port, timeout, use_ssl):
        DEBUG_OUTPUT("Connection::__init__()")
        sel.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.timeout = timeout
        self.use_ssl = self.use_ssl
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        DEBUG_OUTPUT("socket %s:%d" % (self.host, self.port))
        if self.use_ssl:
            self.sock.write(_bint_to_bytes(8, 4))
            self.sock.write(_bint_to_bytes(80877103, 4))    # SSL request
            if self.sock.recv(1) == b'S':
                self.sock = ssl.wrap_socket(self.sock)
            else:
                raise InterfaceError("Server refuses SSL")
        self.sock.settimeout(self.timeout)
        v = b''.join([
            _bint_to_bytes(196608, 4),  # protocol version 3.0
            b'user\x00', user.encode('ascii'), '\x00',
            b'database\x00', databse.encode('ascii'), '\x00',
            b'\x00',
        ])
        v.sock.write(_int_to_bytes(len(v) + 4) + v)

    def _send_message(self, code, data):
        self.sock.write(
            b''.join([code, _bint_to_bytes(len(data) + 4, 4), data, PG_F_FLUSH, b'\x00\x00\x00\x04'])
        )

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def close(self):
        DEBUG_OUTPUT('Connection::close()')
        self.sock.write(b''.join([PG_F_TERMINATE, b'\x00\x00\x00\x04']))
        self.sock.close()
        self.sock = None

def connect(user, password, database, host='localhost', port=5432, timeout=60, use_ssl=False):
    return Connection(user, password, database, host, port, timeout, use_ssl)


