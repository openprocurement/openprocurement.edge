import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

requires = [
    'cornice',
    'gevent',
    'pyramid_exclog',
    'setuptools',
    'couchdb',
    'couchapp',
    'pycrypto',
    'openprocurement_client',
    'munch',
    'tzlocal',
    'pyyaml',
    'psutil',
    'iso8601'
]
test_requires = requires + [
    'requests',
    'webtest',
    'python-coveralls',
    'nose',
    'mock'
]

entry_points = {
    'paste.app_factory': [
        'main = openprocurement.edge.main:main'
    ],
    'console_scripts': [
        'edge_data_bridge = openprocurement.edge.databridge:main'
    ]
}

setup(name='openprocurement.edge',
      version='1.0.0dev7',
      description='openprocurement.edge',
      long_description=README,
      classifiers=[
          "Framework :: Pylons",
          "License :: OSI Approved :: Apache Software License",
          "Programming Language :: Python",
          "Topic :: Internet :: WWW/HTTP",
          "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
      ],
      keywords="web services",
      author='Quintagroup, Ltd.',
      author_email='info@quintagroup.com',
      license='Apache License 2.0',
      url='https://github.com/openprocurement/openprocurement.edge',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['openprocurement'],
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      extras_require={'test': test_requires},
      test_suite="openprocurement.edge.tests.main.suite",
      entry_points=entry_points)
