from setuptools import setup

setup(
   name='llc_queue',
   version='0.1.0',
   author='Fraser Goldsworth',
   author_email='fraser.goldsworth@physics.ox.ac.uk',
   packages=['llc_queue', 'llc_queue.test'],
   scripts=[],
   #url='http://pypi.python.org/pypi/PackageName/',
   license='LICENSE',
   description='A package for managing the LLC queue',
   long_description=open('README.md').read(),
   install_requires=[
       "pytest",
   ],
)
