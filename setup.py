#from ez_setup import use_setuptools
#use_setuptools()
from setuptools import setup, find_packages

# FIXME require paramiko

setup(name="quaffy",
      version="0.1dev",
      description="sftp downloader with couch backend",
      author="Nick Fisher",
      packages = find_packages(),
      zip_safe = True,
      entry_points = {
          'console_scripts': [
              'quaffy = quaffy.quaffy:main',
          ]
      }
     )
