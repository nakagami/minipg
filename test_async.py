##############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2025 Hajime Nakagami
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
import asyncio
import unittest
import minipg

class AsyncTestCase(unittest.TestCase):
    host = 'localhost'
    user = 'postgres'
    password = 'password'
    database = 'test_minipg'

    def test_aio_connect(self):
        async def _test_select():
            conn = await minipg.AsyncConnection.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )

            cur = await conn.cursor()

            await cur.execute("SELECT 42")
            self.assertEqual(cur.rowcount, 1)
            result = await cur.fetchall()
            self.assertEqual(result, [(42,)])
        asyncio.run(_test_select())

    def test_aio_connect_with_loop(self):
        loop = asyncio.new_event_loop()

        async def _test_select():
            conn = await minipg.AsyncConnection.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        
            cur = await conn.cursor()
            await cur.execute("SELECT 42")
            result = await cur.fetchall()
            self.assertEqual(result, [(42, ), ])
            cur.close()
            await conn.close()
        loop.run_until_complete(_test_select())
        loop.close()

    @unittest.skip("create_pool() still not work")
    def test_create_pool(self):
        async def _test_select(loop):
            pool = await minipg.create_pool(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            async with pool.acquire() as conn:
                with await conn.cursor() as cur:
                    await cur.execute("SELECT 42")
                    self.assertEqual(
                        cur.description,
                        [('CONSTANT', 496, 11, 4, 11, 0, False)],
                    )
                    (r,) = await cur.fetchone()
                    self.assertEqual(r, 42)
            pool.close()
            await pool.wait_closed()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_test_select(loop))
        loop.close()

if __name__ == "__main__":
    unittest.main()
