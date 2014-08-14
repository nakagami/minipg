#!/usr/bin/env python
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
              data1          int2,
              data2          int4,
              data3          int8
            )
        """)
        cur.execute("""
            insert into foo (data1, data2, data3) values
                (1, 2, 3), (2, 3, 4), (3, 4, 5)
        """)
        cur.execute("select * from foo")
        cur.execute("update foo set data2 = 0")

if __name__ == "__main__":
    import unittest
    unittest.main()

