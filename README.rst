|tests-passing| |tests-coverage|

.. |tests-passing| image:: https://github.com/kostrykin/pypers/actions/workflows/tests.yml/badge.svg
.. |tests-coverage| image:: https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/kostrykin/5f8b1433a1c405da22639f817d6a38d9/raw/pypers.json

pypers
======

Powerful batch processing using pipelines for orchestration of experiments.

The documentation is available here: https://pypers-batch.readthedocs.io

**Development instructions:**


- Use ``python -m unittest`` in the root directory of the repository to run the test suite.
- Or use coverage.py instead to also produce a test coverage report::

      coverage run -m unittest && python -m coverage html

  This requires `coverage.py <https://coverage.readthedocs.io/en/7.4.0/#quick-start>`_ to be installed, like ``pip install coverage``.

----

Copyright (c) 2017-2024 Leonid Kostrykin, Biomedical Computer Vision Group, Heidelberg University

This work is licensed under the terms of the MIT license.
For a copy, see `LICENSE </LICENSE>`_.
