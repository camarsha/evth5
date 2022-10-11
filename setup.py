from setuptools import setup, find_packages


setup(name='evt_to_h5_converter',
      version='0.1',
      description='Convert NSCLDAQ (boo!!) evt files to h5 format',
      author='Caleb Marshall',
      packages=find_packages(),
      install_requires=['tqdm', 'numpy',
                        'tables', 'sockets', 'faster-filereader']
)
