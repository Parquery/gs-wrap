"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""
import os

from setuptools import setup, find_packages

import gs_wrap_meta

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'README.rst'), encoding='utf-8') as fid:
    long_description = fid.read().strip()  # pylint: disable=invalid-name

setup(
    name=gs_wrap_meta.__title__,
    version=gs_wrap_meta.__version__,
    description=gs_wrap_meta.__description__,
    long_description=long_description,
    url=gs_wrap_meta.__url__,
    author=gs_wrap_meta.__author__,
    author_email=gs_wrap_meta.__author_email__,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    license='License :: OSI Approved :: MIT License',
    keywords='gsutil google cloud storage wrap',
    packages=find_packages(exclude=['tests']),
    install_requires=['asttokens>=1.1.11,<2', 'icontract>=1.5.3,<2', 'google-cloud-storage>=1.12.0,<2'],
    extras_require={
        'dev': [
            'mypy==0.620', 'pylint==1.8.2', 'yapf==0.20.2', 'tox>=3.0.0', 'temppathlib>=1.0.3,<2', 'coverage>=4.5.1,<5',
            'pydocstyle>=2.1.1,<3'
        ]
    },
    py_modules=['gs_wrap', 'gs_wrap_meta'],
    include_package_data=True,
    package_data={
        "gs_wrap": ["py.typed"],
        '': ['LICENSE.txt', 'README.rst'],
    })
