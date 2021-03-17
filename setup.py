from setuptools import setup, find_packages

from os import path

setup(name='graphpipeline',
      use_scm_version={
          "root": ".",
          "relative_to": __file__,
          "local_scheme": "node-and-timestamp"
      },
      setup_requires=['setuptools_scm'],
      description='Framework for managing source data and loading it into Neo4j.',
      url='https://github.com/kaiserpreusse/graphpipeline',
      author='Martin Preusse',
      author_email='martin.preusse@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'requests', 'ftputil', 'graphio', 'python-dateutil', 'bs4'
      ],
      keywords=['data'],
      zip_safe=False,
      classifiers=[
          'Programming Language :: Python',
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers'
      ],
      )
