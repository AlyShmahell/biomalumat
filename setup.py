from distutils.core import setup
setup(
	name='weirdtargets',
	version='0.1.1',
	author='Aly Shmahell',
	author_email='aly.shmahell@gmail.com',
	description='Parallel Processing of a Bioinfomatics Python Objects File.',
	license="Copyrights © 2019 Aly shmahell.",
	packages=['weirdtargets'],
	install_requires=[
			  "ujson==1.35",
			  "toolz==0.9.0",
			  "psutil==5.6.1",
			  "cython==0.29.6",
			  "h5py==2.9.0",
			  "tables==3.5.1"
			  "dask==1.1.4",
			  "bokeh==1.0.4",
			  "numpy==1.16.2",
			  "pandas==0.24.2",
			  "opentargets==3.1.16",
			  "pathos==0.2.3"
			],
	entry_points = {
		'console_scripts': ['closetargets=closetargets.__main__:main'],
      },
)