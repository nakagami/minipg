#!/usr/bin/env python
# coding:utf-8
##############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2014 Hajime Nakagami
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
##############################################################################
import os
import unittest
import io
import decimal
import datetime
import minipg
import ssl


class TestMiniPG(unittest.TestCase):
    host = 'localhost'
    user = 'postgres'
    password = 'password'
    database = 'test_minipg'

    def setUp(self):
        try:
            minipg.create_database(self.database, self.host, self.user, self.password)
        except Exception:
            pass
        if not os.environ.get("GITHUB_ACTIONS"):
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        else:
            ssl_context = None
        self.connection = minipg.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            ssl_context=ssl_context,
        )

    def tearDown(self):
        self.connection.close()

    def test_basic(self):
        cur = self.connection.cursor()
        cur.execute("""
            create temporary table test_basic (
              pk        serial,
              b1        boolean,
              i2        smallint,
              i4        integer,
              i8        bigint,
              dec       decimal(10, 3),
              dbl       double precision,
              s         varchar(255),
              dt        date,
              t1        time,
              t2        time with time zone,
              t3        timestamp,
              t4        timestamp with time zone
            )
        """)
        cur.execute(u"""
            insert into test_basic (b1, i2,i4,i8,dec,dbl,s,dt,t1,t2,t3,t4) values
                (TRUE, 1, 2, 3, 1.1, 2.1, 'あいうえお', '2001-01-01', '04:05:06.789', '04:05:06.789', '2003-04-12 04:05:06.789', '2003-04-12 04:05:06.789'),
                (FALSE, 2, 3, NULL, 1.2, 2.2, 'かきくけこ', 'January 2, 2001', '04:05:06 PST', '04:05:06 PST', '2003-04-12 04:05:06 PST', '2003-04-12 04:05:06 PST'),
                (FALSE, 3, 4, 5, 1.3, 2.3, 'ABC''s', '20010103', '2003-04-12 04:05:06 America/New_York', '2003-04-12 04:05:06 America/New_York', '2003-04-12 04:05:06 America/New_York','2003-04-12 04:05:06 America/New_York')
        """)
        cur.execute("select * from test_basic")
        self.assertEqual(len(cur.fetchall()), 3)
        cur.execute("select * from test_basic")
        self.assertEqual(len(cur.fetchmany(size=2)), 2)

        cur.execute("select count(*) from test_basic where i8 is NULL")
        self.assertEqual(cur.fetchone()[0], 1)
        cur.execute("select i4 from test_basic where i2=%s", (1,))
        self.assertEqual(cur.fetchone()[0], 2)
        cur.execute("select i2, s from test_basic where s=%s", (u'あいうえお',))
        self.assertEqual(cur.fetchone(), (1, u'あいうえお'))
        cur.execute("select count(*) from test_basic where b1=%s", (True,))
        self.assertEqual(cur.fetchone()[0], 1)
        cur.execute("select count(*) from test_basic where b1=%s", (False,))
        self.assertEqual(cur.fetchone()[0], 2)
        cur.execute("select i2 from test_basic where dec=%s", (decimal.Decimal("1.1"),))
        self.assertEqual(cur.fetchone()[0], 1)
        cur.execute("select count(*) from test_basic where dt=%s", (datetime.date(2001, 1, 1),))
        self.assertEqual(cur.fetchone()[0], 1)

        cur.execute("select to_json(test_basic) from test_basic")
        self.assertEqual(len(cur.fetchall()), 3)

        self.connection.commit()
        try:
            cur.execute("E")
        except minipg.DatabaseError as e:
            self.assertEqual(e.code, '42601')
            self.connection.rollback()
        self.connection.execute("select * from test_basic")

    def test_trans(self):
        cur = self.connection.cursor()
        cur.execute("show transaction isolation level")
        self.assertEqual(len(cur.fetchall()), 1)

        cur.execute("""
            create temporary table test_trans (
              pk        serial,
              i2        smallint
            )
        """)
        self.connection.commit()
        # roolback
        cur.execute("insert into test_trans (i2) values (1)")
        cur.execute("select count(*) from test_trans")
        self.assertEqual(cur.fetchone()[0], 1)
        self.connection.rollback()
        cur.execute("select count(*) from test_trans")
        self.assertEqual(cur.fetchone()[0], 0)

        # commit & rollback
        cur.connection.begin()
        cur.execute("insert into test_trans (i2) values (1)")
        cur.execute("select count(*) from test_trans")
        self.assertEqual(cur.fetchone()[0], 1)
        self.connection.commit()
        cur.execute("insert into test_trans (i2) values (2)")
        cur.execute("select count(*) from test_trans")
        self.assertEqual(cur.fetchone()[0], 2)
        self.connection.rollback()
        cur.execute("select count(*) from test_trans")
        self.assertEqual(cur.fetchone()[0], 1)

    def test_autocommit(self):
        self.connection.set_autocommit(True)
        cur = self.connection.cursor()
        cur.execute("""
            create temporary table test_autocommit (
              pk        serial,
              i2        smallint
            )
        """)
        self.connection.commit()
        cur.execute("insert into test_autocommit (i2) values (1)")
        cur.execute("select count(*) from test_autocommit")
        self.assertEqual(cur.fetchone()[0], 1)
        self.connection.rollback()
        cur.execute("select count(*) from test_autocommit")
        self.assertEqual(cur.fetchone()[0], 1)

    def test_function(self):
        cur = self.connection.cursor()
        cur.execute("""
            create or replace function
                test_function(a text, b text, uppercase boolean default false)
            returns text
            as
            $$
             select case
                    when $3 then upper($1 || ' ' || $2)
                    else lower($1 || ' ' || $2)
                    end;
            $$
            language sql immutable strict;
        """)
        cur.execute("SELECT test_function('Hello', 'World', true)")
        self.assertEqual(cur.fetchone()[0], u"HELLO WORLD")
        cur.execute("SELECT test_function('Hello', 'World', false)")
        self.assertEqual(cur.fetchone()[0], u"hello world")
        cur.execute("SELECT test_function('Hello', 'World')")
        self.assertEqual(cur.fetchone()[0], u"hello world")

    def test_binary(self):
        cur = self.connection.cursor()
        cur.execute("""
            create temporary table test_binary (
              pk       serial,
              b        bytea
            )
        """)
        data = bytearray(b'\x00\x01\x02abc')
        cur.execute("insert into test_binary (b) values (%s)", (data,))
        cur.execute("select b from test_binary")
        self.assertEqual(cur.fetchone()[0], data)

    def test_copy(self):
        cur = self.connection.cursor()
        try:
            cur.execute("drop table test_copy")
        except Exception:
            self.connection.rollback()

        cur.execute("""
            create table test_copy (
              pk        serial,
              b1        boolean,
              i2        smallint,
              s         varchar(255)
            )
        """)
        cur.execute(u"""
            insert into test_copy (b1, i2 ,s) values
                (TRUE, 1, 'あいうえお'),
                (FALSE, 2, 'かきくけこ'),
                (FALSE, 3, 'ABC''s')
        """)
        self.assertEqual(cur.rowcount, 3)
        self.connection.commit()

        # COPY TO stdout
        self.connection.close()
        self.connection.reopen()
        text = b"1\tt\t1\t\xe3\x81\x82\xe3\x81\x84\xe3\x81\x86\xe3\x81\x88\xe3\x81\x8a\n2\tf\t2\t\xe3\x81\x8b\xe3\x81\x8d\xe3\x81\x8f\xe3\x81\x91\xe3\x81\x93\n3\tf\t3\tABC's\n"
        f = io.BytesIO()
        self.connection.execute(u"copy test_copy to stdout", f)
        self.assertEqual(text, f.getvalue())

        # COPY FROM stdin
        cur.execute("truncate table test_copy")
        cur.execute("select count(*) from test_copy")
        self.assertEqual(cur.fetchone()[0], 0)
        self.connection.execute(u"copy test_copy from stdin", io.BytesIO(text))
        self.connection.commit()

        # reconnect and check
        self.connection.close()
        self.connection.reopen()
        f = io.BytesIO()
        self.connection.execute(u"copy test_copy to stdout", f)
        self.assertEqual(text, f.getvalue())

        # COPY TO file, COPY FROM file
        self.connection.execute(u"copy test_copy to '/tmp/minipg_test_copy.txt'")
        self.connection.reopen()
        cur.execute("truncate table test_copy")
        cur.execute("select count(*) from test_copy")
        self.assertEqual(cur.fetchone()[0], 0)
        self.connection.execute(u"copy test_copy from '/tmp/minipg_test_copy.txt'")
        f = io.BytesIO()
        self.connection.execute(u"copy test_copy to stdout", f)
        self.assertEqual(text, f.getvalue())
        self.connection.commit()

    def test_japanese(self):
        cur = self.connection.cursor()
        cur.execute(u"""
            create temporary table 日本語 (
              pk        serial,
              文字列    varchar(255)
            )
        """)
        cur.execute(u"insert into 日本語 (文字列) values ('あいうえお')")
        cur.execute(u"select 文字列 from 日本語")
        self.assertEqual(cur.fetchone()[0], u'あいうえお')

    def test_types(self):
        cur = self.connection.cursor()
        cur.execute(u"""
            create temporary table test_serial (
              pk1       serial,
              pk2       bigserial,
              r         real,
              p         point,
              c         circle
            )
        """)
        cur.execute(u"insert into test_serial(r, p, c) values (1.0, point(1.1, 2.2), circle(point(1.1,2.2),3.3))")
        cur.execute(u"select pk1, pk2, r, p, c from test_serial")
        r = cur.fetchone()
        self.assertEqual(r[0], 1)
        self.assertEqual(r[1], 1)
        self.assertEqual(r[2], 1.0)
        self.assertEqual(r[3], (1.1, 2.2))
        self.assertEqual(r[4], ((1.1, 2.2), 3.3))

    def test_isolation_level(self):
        self.assertEqual(self.connection.isolation_level, u'read committed')


if __name__ == "__main__":
    unittest.main()
