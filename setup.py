#!/usr/bin/env python

from distutils.core import setup

with open('pypers/version.py') as fin:
    exec(fin.read(), globals())

setup(
    name = 'repype',
    version = VERSION,
    python_requires = '>3.8',  # requires setuptools>=24.2.0 and pip>=9.0.0
    description = 'Reproducible batch processing using pipelines for scientific computing.',
    author = 'Leonid Kostrykin',
    author_email = 'leonid.kostrykin@bioquant.uni-heidelberg.de',
    url = 'https://github.com/kostrykin/pypers',
    license = 'MIT',
    packages = ['pypers'],
)