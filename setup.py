from setuptools import setup, find_packages
import versioneer

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='unaflow',
    url='https://github.com/unacast/unaflow',
    author='Unacast',
    author_email='ole.christian.langfjaran@unacast.com',
    description='A collection helper classes for Apache Airflow',
    license='Apache License 2.0',
    packages=find_packages(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=[],  # Meant as a helper library. User is required to install.
    python_requires='>=3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
