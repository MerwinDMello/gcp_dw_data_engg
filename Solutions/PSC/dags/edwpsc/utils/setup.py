from setuptools import setup, find_packages

REQUIRED_PACKAGES = ['paramiko']

setup(name='Dependencies',
      description='Dependencies',
      install_requires=[
          'pandas==2.2.3',
          'pandas-gbq==0.26.0',
          'JayDeBeApi==1.2.3',
          'JPype1==1.5.1',
          'SQLAlchemy==2.0.36',
          'oracledb==2.0.1',
          'google-cloud-secret-manager==2.20.2',
          'google-cloud-bigquery==3.20.1',
          'google-api-core==2.19.1',
          'google-cloud-storage==2.18.2',
          'pendulum==3.0.0',
          'paramiko==3.4.0',
          'Jinja2==3.1.4',
          'openpyxl==3.1.5',
          'xlsxwriter==3.2.3',
          'smbprotocol==1.15.0'     
      ],
      packages=find_packages(),
      )