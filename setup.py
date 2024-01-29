import sys
from setuptools import setup, Command


version = "%d.%d.%d" % __import__('minipg').VERSION

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
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
    py_modules=['minipg'],
)
