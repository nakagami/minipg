import sys
from distutils.core import setup, Command
from distutils.extension import Extension

try:
    from Cython.Build import cythonize
    ext_modules = cythonize([
        Extension("minipg.pgcore", ["minipg/pgcore.py"]),
    ])
except ImportError:
    ext_modules = None


class TestCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from minipg import test_minipg
        import unittest
        unittest.main(test_minipg, argv=sys.argv[:1])

cmdclass = {'test': TestCommand}

version = "%d.%d.%d" % __import__('minipg').VERSION

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name="minipg",
    version=version,
    url='https://github.com/nakagami/minipg/',
    classifiers=classifiers,
    keywords=['PostgreSQL'],
    author='Hajime Nakagami',
    author_email='nakagami@gmail.com',
    description='Yet another PostgreSQL database driver',
    long_description=open('README.rst').read(),
    license="MIT",
    packages=['minipg'],
    cmdclass=cmdclass,
    ext_modules=ext_modules,
)
