=============
minipg
=============

Yet another Python PostgreSQL database driver.

Requirements
-----------------

- PostgreSQL 9.6+
- Python 3.9+

Installation
-----------------

use pip
::

    $ pip install minipg

or

copy a module file.
::

    $ cd $(SOMEWHERE_PYTHON_PATH)
    $ wget https://github.com/nakagami/minipg/raw/master/minipg.py

Example
-----------------

::

   import minipg
   conn = minipg.connect(host='localhost',
                       user='postgres',
                       password='secret',
                       database='database_name')
   cur = conn.cursor()
   cur.execute('select foo, bar from baz')
   for r in cur.fetchall():
      print(r[0], r[1])
   conn.close()

SSL Connection
++++++++++++++++++

You can make an SSL connection with an instance of SSLContext.
Below is an example of an ssl connection without certificate validation.

::

   import ssl
   import minipg
   ssl_context = ssl.create_default_context()
   ssl_context.check_hostname = False
   ssl_context.verify_mode = ssl.CERT_NONE
   conn = minipg.connect(host='localhost',
                       user='postgres',
                       password='secret',
                       database='database_name',
                       ssl_context=ssl_context)

Asyncio example
++++++++++++++++++

Please refer to the test code.

https://github.com/nakagami/minipg/blob/master/test_async.py


Restrictions and Unsupported Features
--------------------------------------

- Supported Authentication METHOD are only 'trust', 'md5' and 'scram-sha-256'.
- Not full support for array data types.

For MicroPython
----------------

See https://github.com/nakagami/micropg .
It's a minipg subset driver.
