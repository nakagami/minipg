import sys
from distutils.core import setup, Command
from distutils.extension import Extension

class TestCommand(Command):
    user_options = [ ]

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from minipg import test_minipg
        import unittest
        unittest.main(test_minipg, argv=sys.argv[:1])

cmdclass = {'test': TestCommand}

version_tuple = __import__('minipg').VERSION

if version_tuple[2] is not None:
    version = "%d.%d.%s" % version_tuple
else:
    version = "%d.%d" % version_tuple[:2]

classifiers = [
    'Development Status :: 3 - Alpha',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name = "minipg",
    version = version,
    url = 'https://github.com/nakagami/minipg/',
    classifiers=classifiers,
    keywords=['PostgreSQL'],
    author = 'Hajime Nakagami',
    author_email = 'nakagami@gmail.com',
    description = 'Yet another PostgreSQL database adapter',
    license = "MIT",
    packages = ['minipg'],
    cmdclass = cmdclass,
)
