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
import cython

cdef int PY2
cdef int DEBUG
cdef void DEBUG_OUTPUT(str s)

cdef int PG_TYPE_BOOL
cdef int PG_TYPE_BYTEA
cdef int PG_TYPE_CHAR
cdef int PG_TYPE_NAME
cdef int PG_TYPE_INT8
cdef int PG_TYPE_INT2
cdef int PG_TYPE_INT2VECTOR
cdef int PG_TYPE_INT4
cdef int PG_TYPE_REGPROC
cdef int PG_TYPE_TEXT
cdef int PG_TYPE_OID
cdef int PG_TYPE_TID
cdef int PG_TYPE_XID
cdef int PG_TYPE_CID
cdef int PG_TYPE_VECTOROID
cdef int PG_TYPE_JSON
cdef int PG_TYPE_XML
cdef int PG_TYPE_PGNODETREE
cdef int PG_TYPE_POINT
cdef int PG_TYPE_LSEG
cdef int PG_TYPE_PATH
cdef int PG_TYPE_BOX
cdef int PG_TYPE_POLYGON
cdef int PG_TYPE_LINE
cdef int PG_TYPE_FLOAT4
cdef int PG_TYPE_FLOAT8
cdef int PG_TYPE_ABSTIME
cdef int PG_TYPE_RELTIME
cdef int PG_TYPE_TINTERVAL
cdef int PG_TYPE_UNKNOWN
cdef int PG_TYPE_CIRCLE
cdef int PG_TYPE_CASH
cdef int PG_TYPE_MACADDR
cdef int PG_TYPE_INET
cdef int PG_TYPE_CIDR
cdef int PG_TYPE_NAMEARRAY
cdef int PG_TYPE_INT2ARRAY
cdef int PG_TYPE_INT4ARRAY
cdef int PG_TYPE_TEXTARRAY
cdef int PG_TYPE_ARRAYOID
cdef int PG_TYPE_FLOAT4ARRAY
cdef int PG_TYPE_ACLITEM
cdef int PG_TYPE_CSTRINGARRAY
cdef int PG_TYPE_BPCHAR
cdef int PG_TYPE_VARCHAR
cdef int PG_TYPE_DATE
cdef int PG_TYPE_TIME
cdef int PG_TYPE_TIMESTAMP
cdef int PG_TYPE_TIMESTAMPTZ
cdef int PG_TYPE_INTERVAL
cdef int PG_TYPE_TIMETZ
cdef int PG_TYPE_BIT
cdef int PG_TYPE_VARBIT
cdef int PG_TYPE_NUMERIC
cdef int PG_TYPE_REFCURSOR
cdef int PG_TYPE_REGPROCEDURE
cdef int PG_TYPE_REGOPER
cdef int PG_TYPE_REGOPERATOR
cdef int PG_TYPE_REGCLASS
cdef int PG_TYPE_REGTYPE
cdef int PG_TYPE_REGTYPEARRAY
cdef int PG_TYPE_UUID
cdef int PG_TYPE_TSVECTOR
cdef int PG_TYPE_GTSVECTOR
cdef int PG_TYPE_TSQUERY
cdef int PG_TYPE_REGCONFIG
cdef int PG_TYPE_REGDICTIONARY
cdef int PG_TYPE_INT4RANGE
cdef int PG_TYPE_RECORD
cdef int PG_TYPE_RECORDARRAY
cdef int PG_TYPE_CSTRING
cdef int PG_TYPE_ANY
cdef int PG_TYPE_ANYARRAY
cdef int PG_TYPE_VOID
cdef int PG_TYPE_TRIGGER
cdef int PG_TYPE_EVTTRIGGER
cdef int PG_TYPE_LANGUAGE_HANDLER
cdef int PG_TYPE_INTERNAL
cdef int PG_TYPE_OPAQUE
cdef int PG_TYPE_ANYELEMENT
cdef int PG_TYPE_ANYNONARRAY
cdef int PG_TYPE_ANYENUM
cdef int PG_TYPE_FDW_HANDLER
cdef int PG_TYPE_ANYRANGE

@cython.locals(r=cython.longlong)
cdef long long _bytes_to_bint(bytes b)

cdef bytes _bint_to_bytes(int val)


cdef class Connection:
    cdef user, password, database, host, port, timeout, use_ssl, sock
    cdef _ready_for_query
    cdef public object encoding, encoders, autocommit, tzinfo
    cdef public int server_version

    @cython.locals(t=cython.type)
    cpdef escape_parameter(Connection self, object v)

    @cython.locals(code=cython.int, n=cython.int, ln=cython.int,
                    salt=cython.bytes, hash1=cython.bytes, hash2=cython.bytes,
                    type_code=cython.int,
                    size=cython.int, precision=cython.int, scale=cython.int,
                    field=cython.tuple)
    cdef object _process_messages(Connection self, object obj)

    cdef void process_messages(Connection self, object obj) except *

    cdef void _send_message(Connection self, bytes message, bytes data)
    cdef object _decode_column(Connection self, object data, int oid)

    cdef bytes _read(Connection self, int ln)

    @cython.locals(n=cython.int)
    cdef void _write(Connection self, bytes b)

    @cython.locals(v=bytes)
    cdef void _open(Connection self)

    cpdef begin(Connection self)
    cdef void _execute(Connection self, object query, object obj) except *
    cpdef commit(Connection self)
    cpdef rollback(Connection self)

    cpdef reopen(Connection self)

    cpdef close(Connection self)
