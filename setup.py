# Copyright 2018 Fred Hutchinson Cancer Research Center
# from distutils.core import setup

from setuptools import setup, find_packages


packages = find_packages(exclude=['tests']);


setup(name='parallel_task_database',
      author='Keith Curtis',
      description='Help manage tasks running on a cluster via a database',
      license='License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
      version='1.0',
      url='https://github.com/krcurtis/',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Programming Language :: Python :: 3.5'
          ],
      keywords='cluster',
      packages=packages,
      install_requires=['pymongo', 'numpy'],
      python_requires='>=3.5.3',
      scripts = [ 
          "scripts/check_job_status.py",
          "scripts/clear_database_jobs.py",
          "scripts/reset_uncompleted_jobs.py"
      ]
      )
