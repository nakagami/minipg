import cython

cdef int PY2
cdef int DEBUG
cdef void DEBUG_OUTPUT(str s)

cdef int PG_B_AUTHENTICATION
cdef int PG_B_BACKEND_KEY_DATA
cdef int PG_F_BIND
cdef int PG_B_BIND_COMPLETE
cdef int PG_F_CLOSE
cdef int PG_B_CLOSE_COMPLETE
cdef int PG_B_COMMAND_COMPLETE
cdef int PG_COPY_DATA
cdef int PG_COPY_DONE
cdef int PG_F_COPY_FALL
cdef int PG_B_COPY_IN_RESPONSE
cdef int PG_B_COPY_OUT_RESPONSE
cdef int PG_B_COPY_BOTH_RESPONSE
cdef int PG_B_DATA_ROW
cdef int PG_F_DESCRIBE
cdef int PG_B_EMPTY_QUERY_RESPONSE
cdef int PG_B_ERROR_RESPONSE
cdef int PG_F_EXECUTE
cdef int PG_F_FLUSH
cdef int PG_F_FUNCTION_CALL
cdef int PG_B_FUNCTION_CALL_RESPONSE
cdef int PG_B_NO_DATA
cdef int PG_B_NOTICE_RESPONSE
cdef int PG_B_NOTIFICATION_RESPONSE
cdef int PG_B_PARAMETER_DESCRIPTION
cdef int PG_B_PARAMETER_STATUS
cdef int PG_F_PARSE
cdef int PG_B_PARSE_COMPLETE
cdef int PG_F_PASSWORD_MESSAGE
cdef int PG_B_PORTAL_SUSPEND
cdef int PG_F_QUERY
cdef int PG_B_READY_FOR_QUERY
cdef int PG_B_ROW_DESCRIPTION
cdef int PG_F_SYNC
cdef int PG_F_TERMINATE

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

cdef object _decode_column(data, int oid, encoding, tzinfo, use_tzinfo)

@cython.locals(r=cython.longlong)
cdef long long _bytes_to_bint(bytes b)

cdef bytes _bint_to_bytes(int val)

cpdef escape_parameter(v)

cdef class Connection:
    cdef user, password, database, host, port, timeout, use_ssl, encoding, sock
    cdef tzinfo, use_tzinfo
    cdef int autocommit
    cdef _ready_for_query

    cdef void _send_message(Connection self, int code, bytes data)

    cdef bytes _read(Connection self, int ln)

    @cython.locals(n=cython.int)
    cdef _write(Connection self, bytes b)

    @cython.locals(v=bytes)
    cdef _open(Connection self)

    cpdef begin(Connection self)
    cpdef commit(Connection self)
    cpdef rollback(Connection self)

    cpdef reopen(Connection self)

    cpdef close(Connection self)
