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


Restrictions and Unsupported Features
--------------------------------------

- Python 3.6, 3.7, 3.8 requires backports.zoneinfo https://pypi.org/project/backports.zoneinfo/ install
- Supported Authentication METHOD are only 'trust', 'md5' and 'scram-sha-256'.
- Not full support for array data types.
- Not support for prepared statements.

For MicroPython
----------------

See https://github.com/nakagami/micropg .
It's a minipg subset driver.
