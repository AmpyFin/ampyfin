#
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
#

import os
import sys

# -- Path setup --------------------------------------------------------------
# If your modules are in a directory one level up from docs/, adjust the path:
sys.path.insert(0, os.path.abspath('..'))

# -- Project information -----------------------------------------------------
project = 'AmpyFin'
copyright = '2025, Yeon Lee, Nitin Datta Movva'
author = 'Yeon Lee, Nitin Datta Movva'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release| in the documentation. For example:
#
version = '3.0'           # Short version
release = '3.0.2'           # Full version

# -- General configuration ---------------------------------------------------
#
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration
#

extensions = [
    # Core Sphinx extensions:
    'sphinx.ext.autodoc',      # Automatically pull in docstrings from your modules
    'sphinx.ext.viewcode',     # Add links to highlighted source code
    'sphinx.ext.napoleon',     # Parse Google/NumPy style docstrings
    'sphinx.ext.intersphinx',  # Reference objects in external projects (optional)
    'sphinx.ext.todo',         # Track to-do items (optional)
    'sphinx.ext.autosummary',  # Generate summary tables for modules/classes (optional)
    'sphinx.ext.mathjax',      # Render math via JavaScript (optional)
]

# Napoleon settings (to handle Google or NumPy style docstrings)
# https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False  # If True, include __init__ doc with class doc
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_param = True
napoleon_use_rtype = True

# Autodoc settings
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration
autodoc_default_options = {
    'members': True,              # Include documented members
    'undoc-members': False,       # Set to True to see which members are missing docstrings
    'show-inheritance': True,
    # 'inherited-members': True,   # Optional: include inherited members
}
autoclass_content = 'class'       # 'class' or 'both' (include class and __init__ docstrings)

# If some modules have optional dependencies that break autodoc, you can mock them:
# autodoc_mock_imports = ["pandas", "numpy", "sklearn"]

# This pattern tells Sphinx where to find your reST/Markdown files
templates_path = ['_templates']
exclude_patterns = []

# Optionally specify the master doc (if your main toctree is in something other than "index.rst")
# master_doc = 'index'

# -- Internationalization ----------------------------------------------------
# language = 'en'    # Uncomment for non-English docs

# -- Options for HTML output ------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# You can keep using 'alabaster' or switch to 'sphinx_rtd_theme' for a more familiar RTD look:
# html_theme = 'alabaster'
html_theme = 'sphinx_rtd_theme'  # pip install sphinx-rtd-theme

# Theme-specific options (for alabaster, for example):
html_theme_options = {
    'description': 'Read The Docs theme for AmpyFin',
    'sidebar_width': '220px',
    'page_width': '1020px',
    # see https://alabaster.readthedocs.io/en/latest/customization.html
}

html_static_path = ['_static']

# If you want a custom favicon or logo, place them in '_static' and point here:
html_logo = '_static/logo.png'
# html_favicon = '_static/favicon.ico'

#
# End of conf.py
#
