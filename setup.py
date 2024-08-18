#!/usr/bin/env python

from distutils.core import setup

with open('repype/version.py') as fin:
    exec(fin.read(), globals())


def strip_raw(rst):
    lines_in = rst.split('\n')
    lines_out = list()
    while len(lines_in) > 0:
        line = lines_in.pop(0)

        # Found a raw directive
        if line.strip() == '.. raw:: html':
            raw_length = 0
            while len(lines_in) > 0:
                line = lines_in.pop(0)
                if line == '' and raw_length > 1:
                    break
                else:
                    raw_length += 1

        # Fast-forward
        else:
            lines_out.append(line)

    # Strip empty lines
    while len(lines_out) > 0 and lines_out[0].strip() == '':
        lines_out.pop(0)
    while len(lines_out) > 0 and lines_out[-1].strip() == '':
        lines_out.pop(-1)

    # Strip trailing '----'
    if lines_out[-1] == '----':
        lines_out.pop(-1)
    while len(lines_out) > 0 and lines_out[-1].strip() == '':
        lines_out.pop(-1)

    return '\n'.join(lines_out)


setup(
    name = 'repype',
    version = VERSION,
    python_requires = '>3.8',  # requires setuptools>=24.2.0 and pip>=9.0.0
    description = 'Reproducible batch processing using pipelines for scientific computing.',
    long_description = strip_raw(open('README.rst').read()),
    long_description_content_type = 'text/x-rst',
    author = 'Leonid Kostrykin',
    author_email = 'leonid.kostrykin@bioquant.uni-heidelberg.de',
    url = 'https://github.com/kostrykin/repype',
    license = 'MIT',
    packages = ['repype'],
)