"""
pypi setup
"""
import os

from setuptools import setup


_here = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(os.path.join(_here, 'rasgoql', 'version.py')) as f:
    exec(f.read(), version)

with open(os.path.join(_here, 'DESCRIPTION.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(os.path.join(_here, 'requirements.txt'), encoding='utf-8') as f:
    req_lines = f.read()
    requirements = req_lines.splitlines()

with open(os.path.join(_here, 'requirements_snowflake.txt'), encoding='utf-8') as f:
    req_lines = f.read()
    sf_requirements = req_lines.splitlines()

setup(
    name='rasgoql',
    version=version['__version__'],
    description=('Alpha version of rasgoQL open-source package.'),
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Rasgo Intelligence',
    author_email='patrick@rasgoml.com',
    project_urls={
    'Documentation': 'https://docs.rasgoql.com',
    'Source': 'https://github.com/rasgointelligence/RasgoQL',
    'Rasgo': 'https://www.rasgoml.com/',
    },
    license='GNU Affero General Public License v3 or later (AGPLv3+)',
    packages=[
        'rasgoql',
        'rasgoql.data',
        'rasgoql.primitives',
        'rasgoql.utils'
        ],
    install_requires=requirements,
    extras_require={
        "snowflake":  sf_requirements,
    },
    include_package_data=True,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Database',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Code Generators'
        ]
)
