.. raw:: html

  <div align="center">
    <h6>To support the sustainability of your software experiments</h6>
    <h1>
      <a href="https://github.com/kostrykin/repype">repype</a><br>
      <a href="http://pypi.org/p/repype"><img alt="PyPI version" src="https://img.shields.io/pypi/v/repype"></a>
      <a href="https://anaconda.org/conda-forge/repype"><img alt="conda-forge version" src="https://img.shields.io/conda/vn/conda-forge/repype.svg"></a>
      <a href="https://anaconda.org/conda-forge/repype"><img src="https://img.shields.io/conda/dn/conda-forge/repype.svg?label=conda%20Downloads" /></a><br>
      <a href="https://github.com/kostrykin/repype/actions/workflows/tests.yml"><img src="https://github.com/kostrykin/repype/actions/workflows/tests.yml/badge.svg" /></a>
      <a href="https://github.com/kostrykin/repype/actions/workflows/tests.yml"><img src="https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/kostrykin/5f8b1433a1c405da22639f817d6a38d9/raw/pypers.json" /></a>
      <a href="https://repype.readthedocs.io"><img src="https://readthedocs.org/projects/repype/badge/?version=latest" /></a><br>
    </h1>
  </div>

**Installation:**

.. code::

    git clone git@github.com:kostrykin/repype.git
    cd repype && python setup.py install

**Documentation:** https://repype.readthedocs.io

**Examples:**

- https://github.com/kostrykin/repype/tree/master/examples
- https://github.com/BMCV/SuperDSM

**Development instructions:**

- To run the test suite, first install the testing dependencies::

      pip install -r tests/requirements.txt
      python -m unittest

- Instead of using ``python -m unittest``, use coverage.py to also produce a test coverage report::

      coverage run -m unittest && coverage combine && coverage html

  This requires `coverage.py <https://coverage.readthedocs.io/en/7.4.0/#quick-start>`_ to be installed additionally, like ``pip install coverage``.

- To build the documentation locally::

      pip install -r docs/requirements.txt
      cd docs
      make html

  You can then open ``build/html/index.html`` to view the documentation.

- To run the Textual interface with debug console::

      textual run --dev repype.textual.app:Repype

  after running ``textual console`` on a different terminal.

----

.. raw:: html

  <div align="center">
    Copyright (c) 2017-2025 Leonid Kostrykin, Biomedical Computer Vision Group, Heidelberg University<br>
    This work is licensed under the terms of the MIT license. For a copy, see <a href="LICENSE">LICENSE</a>.
  </div>
