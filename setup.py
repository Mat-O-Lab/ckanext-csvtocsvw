# -*- coding: utf-8 -*-
from setuptools import setup, find_packages  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))


# Get the long description from the relevant file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='''ckanext-csvwtocsvw''',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # http://packaging.python.org/en/latest/tutorial.html#version
    version='0.0.1',

    description='''Extension automatically generating csvw metadata for uploaded textual tabular data.''',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/Mat-O-Lab/ckanext-csvtocsvw',

    # Author details
    author='''Thomas Hanke''',
    author_email='''thomas.hanke@iwm.fraunhofer.de''',

    # Choose your license
    license='AGPL',
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    },
    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points='''
        [ckan.plugins]
        csvtocsvw=ckanext.csvtocsvw.plugin:CsvtocsvwPlugin
    ''',
    
)
