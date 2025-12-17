import sys
import os
sys.path.insert(0, os.path.abspath('../../src'))

import OceanDB.OceanDB

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'OceanDB'
copyright = '2025, Matthias Darr, Jeffrey Early, Daniel Neshyba-Rowe, Cimarron Wortham'
author = 'Matthias Darr, Jeffrey Early, Daniel Neshyba-Rowe, Cimarron Wortham'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
        'sphinx.ext.autodoc',
        'sphinx.ext.autosummary',
        'sphinx_autodoc_typehints',
        # 'sphinx.rtd_theme',
        ]

templates_path = ['_templates']
exclude_patterns = []



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
