#!/usr/bin/env python

from distutils.core import setup

with open('pypers/version.py') as fin:
    exec(fin.read(), globals())

setup(
    name = 'pypers',
    version = VERSION,
    description = 'Powerful batch processing using pipelines for scientific computing.',
    author = 'Leonid Kostrykin',
    author_email = 'leonid.kostrykin@bioquant.uni-heidelberg.de',
    url = 'https://github.com/kostrykin/pypers',
    license = 'MIT',
    packages = ['pypers'],
)