# Copyright 2017 Ludwig Schubert.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

from setuptools import setup, find_packages

version = '0.0.46'

setup(
  name = 'flow-simulator',
  packages = find_packages(exclude=[]),
  version = version,
  description = ('Continuous integration for research.'),
  author = 'Ludwig Schubert',
  author_email = 'ludwigschubert@google.com',
  url = 'https://github.com/ludwigschubert/flow-simulator',
  download_url = ('https://github.com/ludwigschubert/flow-simulator'
    '/archive/v{}.tar.gz'.format(version)),
  license = 'Apache License 2.0',
  keywords = ['continuous integration', 'make'],
  install_requires = [
    'absl-py',
    'watchdog',
    'decorator',
    'lucid',
    'toposort',
    'google-api-python-client==1.6.5',
    'google-auth==1.4.1',
    'google-auth-httplib2==0.0.3',
    'google-cloud-datastore==1.6.0',
    'google-cloud-storage',
  ],
  classifiers = [
    'Development Status :: 2 - Pre-Alpha',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: Apache Software License',
    'Natural Language :: English',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Topic :: Scientific/Engineering',
    'Topic :: Software Development :: Libraries :: Python Modules',
  ],
)
