from setuptools import setup, find_packages
from setuptools.command.install import install

class CustomInstall(install):
    def run(self):
        super().run()

setup(name='hsi_xfer',
      version='1.1.0',
      description='A wrapper for HSI that enforces ordered recall from tape, and increases ease of use. Think: rsync for HPSS',
      author='hsi_xfer',
      author_email='wynnejr@ornl.gov',
      url='https://gitlab.ccs.ornl.gov/archive-team/hsi_xfer',
      license='UNLICENSED',
      zip_safe=False,
      python_requires='>=3.7',
      packages=find_packages(),
      entry_points={
          'console_scripts': [
              'hsi_xfer = hsi_xfer.__main__:entrypoint'
          ]
      },
      cmdclass={'install': CustomInstall},
)
