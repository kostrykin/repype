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

.. _usage_example:

Example
*******

Pipeline stages require different inputs and produce different outputs. These are like intermediate results, which are shared or passed between the stages. The pipeline maintains their state, which is kept inside the *pipeline data object*.

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

For details, see below.

.. _batch_system:

Batch processing
****************

.. _batch_task_spec:

Task specification
^^^^^^^^^^^^^^^^^^

To perform batch processing of a dataset, you first need to create a *task*. To do that, create an empty directory, and put a ``task.json`` file in it. This file will contain the specification of the segmentation task. Below is an example specification:

.. code-block:: json

   {
       "runnable": true,

       "input_pathpattern": "/data/dataset/img-%d.tiff",
       "result_pathpattern": "seg/dna-%d.png",
       
       "inputs": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],

       "config": {
       }
   }

The meaning of the different fields is as follows:

``runnable``
    Marks this task as runnable (or not runnable). If set to ``false``, the specification will be treated as a template for derived tasks. Derived tasks are placed in sub-folders and inherit the specification of the parent task. This is useful, for example, if you want to try out different hyperparameters. The batch system automatically picks up intermediate results of parent tasks to speed up the completion of derived tasks.

``environ``
    Defines environment variables which are to be set. In the example above, MKL and OpenBLAS numpy backends are both instructed to use two threads for parallel computations.

``input_pathpattern``
    Defines the path to the input files, using placeholders like ``%d`` for decimals and ``%s`` for strings (decimals can also be padded with zeros to a fixed length using, e.g., use ``%02d`` for a length of 2).

``result_pathpattern``
    Relative path of files, where the results are written, using placeholders as described above.

``log_pathpattern``
    Relative path of files, where the logs are to be written to, using placeholders as described above (mainly for debugging purposes).

``cfg_pathpattern``
    Relative path of files, where the hyperparameters are to be written to, using placeholders as described above (mainly for reviewing the automatically generated hyperparameters).

``inputs``
    List of inputs, which are used to resolve the pattern-based fields described above. In the considered example, the list of input images will resolve to ``/data/dataset/img-1.tiff``, …, ``/data/dataset/img-10.tiff``. Inputs are allowed to be strings, and they are also allowed to contain ``/`` to encode paths which involve sub-directories.

``last_stage``
    If specified, then the pipeline processing will end at the specified stage.

``config``
    Defines the hyperparameters to be used. Note that namespaces must be specified as nested JSON objects.

Instead of specifying the hyperparameters in the task specification directly, it is also possible to include them from a separate JSON file using the ``base_config_path`` field. The path must be either absolute or relative to the ``task.json`` file. It is also possible to use ``{DIRNAME}`` as a substitute for the name of the directory, which the ``task.json`` file resides in. The placeholder ``{ROOTDIR}`` in the path specification resolves to the *root directory* passed to the batch system (see below).

.. _batch_prcessing:

Batch processing
^^^^^^^^^^^^^^^^

To perform batch processing of all tasks specified in the current working directory, including all sub-directories and so on:

.. code-block:: console

   python -m batch .

This will run the batch system in *dry mode*, so nothing will actually be processed. Instead, each task which is going to be processed will be printed, along with some additional information. To actually start the processing, re-run the command and include the ``--run`` argument.

.. note::
    In this example, the current working directory will correspond to the *root directory* when it comes to resolving the ``{ROOTDIR}`` placeholder in the path specification.

Note that the batch system will automatically skip tasks which already have been completed in a previous run, unless the ``--force`` argument is used. On the other hand, tasks will not be marked as completed if the ``--oneshot`` argument is used. To run only a single task from the root directory, use the ``--task`` argument, or ``--task-dir`` if you want to automatically include the dervied tasks. Note that, in both cases, the tasks must be specified relatively to the root directory.
