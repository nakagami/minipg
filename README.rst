=============
minipg
=============

Yet another Python PostgreSQL database driver.

Pure python or Cython http://cython.org/ accelaleted.

Requirements
-----------------

- PostgreSQL 9.6+
- Python 3.5+

Installation
-----------------

::

    $ pip install minipg

Example
-----------------

Query::

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

COPY TO::

   import minipg
   conn = minipg.connect(host='localhost',
                       user='postgres',
                       password='secret',
                       database='database_name')
   f = open('foo.txt', 'bw')
   conn.execute('COPY foo_table TO stdout', f)
   f.close()
   conn.close()

COPY FROM::

   import minipg
   conn = minipg.connect(host='localhost',
                       user='postgres',
                       password='secret',
                       database='database_name')
   f = open('foo.txt', 'br')
   conn.execute('COPY foo_table FROM stdin', f)
   f.close()
   conn.close()

As sqlalchemy dialect::

   from sqlalchemy.dialects import registry
   registry.register("postgresql.minipg", "minipg.dialect", "PGDialect_minipg")

   from sqlalchemy import create_engine
   engine = create_engine('postgresql+minipg://postgres:secret@host/database_name')


Restrictions and Unsupported Features
--------------------------------------

- Supported Authentication METHOD are only 'trust', 'md5' and 'scram-sha-256'.
- Not full support for array data types.
- Not support for prepared statements.

For MicroPython
----------------

See https://github.com/nakagami/micropg .
It's a minipg subset driver.
