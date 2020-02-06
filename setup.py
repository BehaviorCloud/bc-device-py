from distutils.core import setup
setup(
  name = 'bc-analyzer',
  packages = ['behaviorcloud.analyzer'],
  version = '0.1.1',
  license='MIT',
  description = 'A package to create a BehaviorCloud compatible data analyzer.',
  author = 'Christian Lent',
  author_email = 'christian@behaviorcloud.com',
  url = 'https://github.com/behaviorcloud/bc-analyzer-py',
  download_url = 'https://github.com/behaviorcloud/bc-analyzer-py/archive/v0.1.1.tar.gz',
  keywords = ['behaviorcloud', 'behavior', 'cloud', 'analyzer', 'analyze', 'analysis', 'data', 'daemon', 'api'],
  install_requires=[
    'python-dateutil',
    'requests',
    'sentry-sdk',
  ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
  ],
)