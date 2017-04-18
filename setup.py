import os
import re
from incline import __version__
from setuptools import setup, find_packages

requires = [
    'boto3>=1.4.1,<1.5.0'
]

setup(
    name='incline',
    version=__version__,
    author="Chris Maxwell",
    author_email="chris@wrathofchris.com",
    description="Incline (RAMP) Read Atomic MultiPartition Transactions",
    url="https://github.com/WrathOfChris/incline",
    download_url='https://github.com/WrathOfChris/incline/tarball/%s' % __version__,
    include_package_data=True,
    packages=find_packages(
        exclude=['tests']),
    install_requires=requires,
    setup_requires=requires,
    )
