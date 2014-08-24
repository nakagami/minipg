=============
minipg
=============

Yet another Python PostgreSQL database driver

Requirements
-----------------

- Python 2.7, 3.3+


Installation
-----------------

Install as a package

::

    $ pip install minipg

Install as a module

::

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


Restrictions and Unsupported Features
--------------------------------------

- Authentication METHOD only can 'trust' or  'md5' in pg_hba.conf.
- Not support array data types.
- Not support prepared statements.
