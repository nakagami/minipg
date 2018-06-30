# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""
sqlalchemy minipg dialect.

::

    from sqlalchemy.dialects import registry
    registry.register("postgresql.minipg", "minipg.dialect", "PGDialect_minipg")

    engine = create_engine('postgresql+minipg://user:password@host/database')

"""


import decimal
import re

from sqlalchemy import exc, processors, util
from sqlalchemy.types import Numeric, JSON as Json
from sqlalchemy.sql.elements import Null
from sqlalchemy.dialects.postgresql.base import PGDialect, PGCompiler, PGIdentifierPreparer, \
    _DECIMAL_TYPES, _FLOAT_TYPES, _INT_TYPES, UUID
from sqlalchemy.dialects.postgresql.hstore import HSTORE
from sqlalchemy.dialects.postgresql.json import JSON, JSONB


class _PGNumeric(Numeric):

    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if not isinstance(coltype, int):
            coltype = coltype.oid
        if self.asdecimal:
            if coltype in _FLOAT_TYPES:
                return processors.to_decimal_processor_factory(
                    decimal.Decimal,
                    self._effective_decimal_return_scale)
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                # PyGreSQL returns Decimal natively for 1700 (numeric)
                return None
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype)
        else:
            if coltype in _FLOAT_TYPES:
                # PyGreSQL returns float natively for 701 (float8)
                return None
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                return processors.to_float
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype)


class _PGHStore(HSTORE):

    def bind_processor(self, dialect):
        if not dialect.has_native_hstore:
            return super(_PGHStore, self).bind_processor(dialect)
        hstore = dialect.dbapi.Hstore
        def process(value):
            if isinstance(value, dict):
                return hstore(value)
            return value
        return process

    def result_processor(self, dialect, coltype):
        if not dialect.has_native_hstore:
            return super(_PGHStore, self).result_processor(dialect, coltype)

    def result_processor(self, dialect, coltype):
        if not dialect.has_native_json:
            return super(_PGJSONB, self).result_processor(dialect, coltype)



class _PGCompiler(PGCompiler):

    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + " %% " + \
            self.process(binary.right, **kw)

    def post_process_text(self, text):
        return text.replace('%', '%%')


class _PGIdentifierPreparer(PGIdentifierPreparer):

    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace('%', '%%')


class PGDialect_minipg(PGDialect):

    driver = 'minipg'

    statement_compiler = _PGCompiler
    preparer = _PGIdentifierPreparer

    @classmethod
    def dbapi(cls):
        return __import__('minipg')

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            Numeric: _PGNumeric,
            HSTORE: _PGHStore,
        }
    )

    def __init__(self, **kwargs):
        super(PGDialect_minipg, self).__init__(**kwargs)
        self.has_native_hstore = False
        self.has_native_json = False
        self.has_native_uuid = False

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['host'] = '%s:%s' % (
                opts.get('host', '').rsplit(':', 1)[0], opts.pop('port'))
        opts.update(url.query)
        return [], opts

    def is_disconnect(self, e, connection, cursor):
        return connection.is_connect()


dialect = PGDialect_minipg
