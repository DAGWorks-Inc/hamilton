#!/usr/bin/env python
# -*- coding: utf-8 -*-
import warnings

"""The setup script."""

from setuptools import setup, find_packages

# don't fail if there are problems with the readme (happens within circleci)
try:
    with open('README.md') as readme_file:
        readme = readme_file.read()
except Exception:
    warnings.warn('README.md not found')
    readme = None

REQUIREMENTS_FILES = [
    'requirements.txt'
]


def get_version():
    version_dict = {}
    with open('hamilton/version.py') as f:
        exec(f.read(), version_dict)
    return '.'.join(map(str, version_dict['VERSION']))


VERSION = get_version()


def load_requirements():
    requirements = set()
    for requirement_file in REQUIREMENTS_FILES:
        with open(requirement_file) as f:
            requirements.update(line.strip() for line in f)
    return list(requirements)


def load_test_requirements():
    with open('requirements-test.txt') as f:
        return [line.strip() for line in f]


setup_requirements = [
    'pytest-runner',
]

test_requirements = load_test_requirements()

setup(
    name='sf-hamilton',  # there's already a hamilton in pypi
    version=VERSION,
    description='Hamilton, the micro-framework for creating dataframes.',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Stefan Krawczyk, Elijah ben Izzy',
    author_email='skrawczyk@stitchfix.com,elijah.benizzy@stitchfix.com',
    url='https://github.com/stitchfix/hamilton',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=load_requirements(),
    zip_safe=False,
    keywords='hamilton',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    test_suite='tests',

    # similar to setup_requires, these packages are not added to your venv but are made available
    # during testing
    tests_require=test_requirements,

    # Any packages required when running `python setup.py X` (where X is an alias). These packages
    # are NOT installed as part of the virtualenv (thus not polluting your venv) but instead just
    # made available to the setup.
    setup_requires=setup_requirements,

    # Note that this feature requires pep8 >= v9 and a version of setup tools greater than the
    # default version installed with virtualenv. Make sure to update your tools!
    python_requires='>=3.6, <4',

    # adding this to slim the package down, since these dependencies are only used to visualize the DAG.
    extras_require={'visualization': ['graphviz', 'networkx'],
                    'dask': ['dask[complete]'],  # commonly you'll want everything.
                    'dask-array': ['dask[array]'],
                    'dask-core': ['dask-core'],
                    'dask-dataframe': ['dask[dataframe]'],
                    'dask-diagnostics': ['dask[diagnostics]'],
                    'dask-distributed': ['dask[distributed]'],
                    'ray': ['ray'],
                    'pyspark': ['pyspark[pandas_on_spark]'],
                    },

    # Relevant project URLs
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/stitchfix/hamilton/issues',
        'Source': 'https://github.com/stitchfix/hamilton',
    },
)
