from setuptools import setup, find_packages

from os import path

setup(name='graphpipeline',
      version='0.0.1',
      description='Framework for managing source data and loading it into Neo4j.',
      url='https://github.com/kaiserpreusse/graphpipeline',
      author='Martin Preusse',
      author_email='martin.preusse@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'requests', 'ftputil', 'graphio', 'python-dateutil'
      ],
      keywords=['data'],
      zip_safe=False,
      classifiers=[
          'Programming Language :: Python',
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers'
      ],
      )
