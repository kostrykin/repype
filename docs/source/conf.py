# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'repype'
copyright = '2017-2024 Leonid Kostrykin, Biomedical Computer Vision Group, Heidelberg University'
author = 'Leonid Kostrykin'

# -- Add directory which contains the project to sys.path
import os, sys
sys.path.insert(0, os.path.abspath('../..'))
os.environ['PYTHONPATH'] = os.path.abspath('../..') + ':' + os.environ.get('PYTHONPATH', '')

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx_autorun',
    'nbsphinx',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

html_static_path = ['_static']

html_css_files = [
    'custom.css',
]

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'

# -- Options for nbsphinx

nbsphinx_execute = 'always'

# -- Report broken links
nitpicky = True
nitpick_ignore = [

    # Standard library

    ('py:class', 'asyncio.events.AbstractEventLoop'),
    ('py:class', 'multiprocessing.context.Process'),

    # WatchDog

    ('py:class', 'watchdog.events.DirModifiedEvent'),
    ('py:class', 'watchdog.events.FileModifiedEvent'),
    ('py:class', 'watchdog.events.FileSystemEventHandler'),

    # Textual
    ('py:class', 'BindingType'),
    ('py:class', 'CSSPathType'),
    ('py:class', 'Screen'),
    ('py:class', 'textual.app.App'),
    ('py:class', 'textual.binding.Binding'),
    ('py:class', 'textual.containers.Vertical'),
    ('py:class', 'textual.screen.ModalScreen'),
    ('py:class', 'textual.screen.Screen'),
    ('py:class', 'textual.widget.Widget'),
    ('py:class', 'textual.widgets._collapsible.Collapsible'),
    ('py:class', 'textual.widgets._input.Input'),
    ('py:class', 'textual.widgets._label.Label'),
    ('py:class', 'textual.widgets._progress_bar.ProgressBar'),
    ('py:class', 'textual.widgets._text_area.TextArea'),

]