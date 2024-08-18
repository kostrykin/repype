.. raw:: html

  <div align="center">
    <h6>To support the sustainability of your software experiments</h6>
    <h1>
      <a href="https://github.com/kostrykin/repype">repype</a><br>
      <a href="http://pypi.org/p/repype"><img alt="PyPI - Version" src="https://img.shields.io/pypi/v/repype"></a>
      <a href="https://github.com/kostrykin/repype/actions/workflows/tests.yml"><img src="https://github.com/kostrykin/repype/actions/workflows/tests.yml/badge.svg" /></a>
      <a href="https://github.com/kostrykin/repype/actions/workflows/tests.yml"><img src="https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/kostrykin/5f8b1433a1c405da22639f817d6a38d9/raw/pypers.json" /></a>
      <a href="https://repype.readthedocs.io"><img src="https://readthedocs.org/projects/repype/badge/?version=latest" /></a><br>
    </h1>
  </div>

| ⚠️ **This branch is currently not stable.** ⚠️
| See https://github.com/kostrykin/repype/releases/tag/v0.1.0 for the last stable version.

----

**Installation:**

.. code::

    git clone git@github.com:kostrykin/repype.git
    cd repype && python setup.py install

**Documentation:** https://repype.readthedocs.io

**Development instructions:**

- To run the test suite, first install the testing dependencies::

      pip install -r tests/requirements.txt
      python -m unittest

- Instead of using ``python -m unittest``, use coverage.py to also produce a test coverage report::

      coverage run -m unittest && python -m coverage html

  This requires `coverage.py <https://coverage.readthedocs.io/en/7.4.0/#quick-start>`_ to be installed additionally, like ``pip install coverage``.

- To build the documentation locally::

      pip install -r docs/requirements.txt
      python -m sphinx -T -W -b html docs/source docs/build -d docs/build -D language=en

  You can then open ``docs/build/index.html`` to view the documentation.

----

.. raw:: html

  <div align="center">
    Copyright (c) 2017-2024 Leonid Kostrykin, Biomedical Computer Vision Group, Heidelberg University<br>
    This work is licensed under the terms of the MIT license. For a copy, see <a href="LICENSE">LICENSE</a>.
  </div>
