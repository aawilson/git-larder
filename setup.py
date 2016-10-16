from setuptools import find_packages, setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'VERSION')) as v:
    VERSION = v.readline().strip()

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="GitLarder",
    version=VERSION,
    description="Python NoSQL Implementation on top of git",
    long_description=long_description,
    url="https://github.com/aawilson/git-larder",
    author="Aaron Wilson",
    author_email="aaron@olark.com",
    license='Apache Software License',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # "Development Status :: 1 - Planning",
        # "Development Status :: 2 - Pre-Alpha",
        # "Development Status :: 3 - Alpha",
        "Development Status :: 4 - Beta",
        # "Development Status :: 5 - Production/Stable",
        # "Development Status :: 6 - Mature",
        # "Development Status :: 7 - Inactive",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        # "Programming Language :: Python :: 3",
        # "Programming Language :: Python :: 3.3",
        # "Programming Language :: Python :: 3.4",
        # "Programming Language :: Python :: 3.5",
    ],
    keywords='git storage nosql',
    packages=find_packages(exclude=['docs', 'tests']),
    install_requires=['GitPython>=0.3.0'],
    extras_require={
        'test': ['nosetests'],
    },
)
