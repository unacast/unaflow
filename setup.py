from setuptools import setup, find_packages
import versioneer

setup(
    name='unaflow',
    url='https://github.com/unaflow.git',
    author='Unacast',
    author_email='developers+github@unacast.com',
    description='A collection of nice to have Airflow classes',
    packages=find_packages(),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    install_requires=[],
)
