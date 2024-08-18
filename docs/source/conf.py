# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'repype'
copyright = '2017-2024 Leonid Kostrykin, Biomedical Computer Vision Group, Heidelberg University'
author = 'Leonid Kostrykin'

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx_autorun',
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

# -- Report broken links
nitpicky = True
nitpick_ignore = [
    ('py:class', 'watchdog.events.FileSystemEventHandler'),
    ('py:class', 'watchdog.events.DirModifiedEvent'),
    ('py:class', 'watchdog.events.FileModifiedEvent'),
    ('py:class', 'DirModifiedEvent'),
    ('py:class', 'FileModifiedEvent'),
]