from setuptools import setup

with open('LICENSE') as f:
    license = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='biomalumat',
    version=1.0,
    author='Aly Shmahell',
    license=license,
    packages=['biomalumat'],
    install_requires=required
)