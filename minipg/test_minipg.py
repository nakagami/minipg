#!/usr/bin/env python
# coding:utf-8
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
import minipg
import unittest
import minipg

class TestMiniPG(unittest.TestCase):
    user='postgres'
    password=''
    database='test_minipg'

    def setUp(self):
        self.connection = minipg.connect(
                            user=self.user,
                            password=self.password,
                            database=self.database)
    def tearDown(self):
        self.connection.close()

    def test_basic(self):
        conn = self.connection
        cur = conn.cursor()
        cur.execute("""
            create temporary table foo (
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
              t2        time with time zone
            )
        """)
        cur.execute(u"""
            insert into foo (b1, i2,i4,i8,dec,dbl,s,dt,t1,t2) values
                (TRUE, 1, 2, 3, 1.1, 2.1, 'あいうえお', '2001-01-01', '04:05:06.789', '04:05:06.789'),
                (FALSE, 2, 3, 4, 1.2, 2.2, 'かきくけこ', 'January 2, 2001', '04:05:06 PST', '04:05:06 PST'),
                (FALSE, 3, 4, 5, 1.3, 2.3, 'さしすせそ', '20010103', '2003-04-12 04:05:06 America/New_York', '2003-04-12 04:05:06 America/New_York')
        """)
        cur.execute("select * from foo")

if __name__ == "__main__":
    import unittest
    unittest.main()

