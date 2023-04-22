Quick start
===========

.. _installation:

Installation
------------

To use pypers, first install it using conda:

.. code-block:: console

   conda install -c bioconda pypers

Usage
-----

.. _usage_example_batch:

Batch processing
****************

To use batch processing, create a file ``batch.py`` with the content,

.. code-block:: python

   import pypers.batch
   import pypers.pipeline

   class Task(pypers.batch.Task):

       def create_pipeline(self, dry):
           return pypers.pipeline.create_pipeline([
               # list of pipeline stages
           ])

   if __name__ == '__main__':
       pypers.batch.run_cli(Task)

and then run from command line:

.. code-block:: console

   python -m batch --help

For details, see :ref:`batch_system`.

