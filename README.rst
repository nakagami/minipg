=============
minipg
=============

Yet another Python PostgreSQL database driver.

Pure python or Cython http://cython.org/ accelaleted.

Requirements
-----------------

- PostgreSQL 9.6+
- Python 2.7, 3.4+, PyPy
- pytz https://pypi.python.org/pypi/pytz

Installation
-----------------

It can install as package or module.

Install as a package

::

    $ pip install -r requirements.txt
    $ pip install minipg

Install as a module

::

    $ pip install pytz
    $ cd $(PROJECT_HOME)
    $ wget https://github.com/nakagami/minipg/raw/master/minipg/minipg.py

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

Execute Query from a command line
-----------------------------------

::

   $ python -m minipg -H pg_server -U user -W pass -D db_name <<EOS
   > SELECT * FROM FOO
   > EOS

or

::

   $ echo 'SELECT * FROM FOO' | python -m minipg -H pg_server -U user -W pass -D db_name

or

::

   $ python -m minipg -H pg_server -U user -W pass -D db_name -Q 'SELECT * FROM FOO'


Restrictions and Unsupported Features
--------------------------------------

- Authentication METHOD only can 'trust' or  'md5' in pg_hba.conf.
- Not full support for array data types.
- Not support for prepared statements.


For MicroPython
----------------

See https://github.com/nakagami/micropg .
It's a minipg subset driver.
