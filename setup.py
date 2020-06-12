from setuptools import setup, find_packages
import versioneer

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='unaflow',
    url='https://github.com/unaflow.git',
    author='Unacast',
    author_email='developers+github@unacast.com',
    description='A collection of nice to have Airflow classes',
    packages=find_packages(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[],  # Meant as a helper library. User is required to install.
)
