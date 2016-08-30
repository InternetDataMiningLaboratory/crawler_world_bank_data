# -*- coding: utf-8 -*-
#
# Author: jimin.huang
#
from setuptools import setup, find_packages

setup(
    name="world_bank",
    version='1.0',
    packages=find_packages(),

    install_requires=[
        'arrow>=0.7.0',
        'mock>=2.0.0',
        'MySQL-python>=1.2.5',
        'nose>=1.3.7',
        'PyYAML>=3.11',
        'Scrapy>=1.0.5',
        'Sphinx>=1.4.1',
        'sphinx-rtd-theme>=0.1.9',
        'SQLAlchemy>=1.0.13',
        'scrapyd>=1.1.0',
        'scrapyd-client>=1.0.1',
        'coverage>=4.1',
        'docopt>=0.6.2',
    ],

    package_data={
        '': ['*.yml'],
    },

    entry_points={
        'scrapy': ['settings=world_bank.settings'],
    }
)
