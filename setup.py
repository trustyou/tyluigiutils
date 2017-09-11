#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import os.path

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()


def read(fname):
    with open(fname) as fp:
        content = fp.read()
    return content

REQUIREMENTS_FOLDER = os.getenv('REQUIREMENTS_PATH', '')

requirements = [line.strip() for line in open(os.path.join(REQUIREMENTS_FOLDER, "requirements.txt"), 'r')]

test_requirements = [line.strip() for line in open(os.path.join(REQUIREMENTS_FOLDER, "requirements_dev.txt"), 'r')]

setup_requirements = [
    'pytest-runner',
]

test_requirements = [
    'pytest',
    # TODO: put package test requirements here
]

setup(
    name='tyluigiutils',
    version='0.2.0',
    description="Misc Luigi related code used by TrustYou ",
    long_description=readme + '\n\n' + history,
    author="Miguel Cabrera",
    author_email='mfcabrera@gmail.com',
    url='https://github.com/mfcabrera/tyluigiutils',
    packages=find_packages('.'),
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='tyluigiutils',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
