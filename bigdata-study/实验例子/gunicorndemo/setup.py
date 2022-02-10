# -*- coding: utf-8 -*-
'''
Created on

@author:
'''

from setuptools import find_packages, setup


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='gunicorndemo',
    version='0.0.1',
    packages=find_packages(),
    description='gunicorndemo Flask Service',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'flask'
    ],
    python_requires='>=3.6',
)