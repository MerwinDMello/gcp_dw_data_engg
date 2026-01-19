from setuptools import setup, find_packages
setup(name='Dependencies',
      description='Dependencies',
      install_requires=[
          'google-cloud-bigquery==3.20.1',
          'google-cloud-storage==2.10.0',
          'google-cloud-secret-manager==2.20.2',
          'pandas==2.1.4',
          'pandas-gbq==0.23.1',
          'JayDeBeApi==1.2.3',
          'pendulum==3.0.0',
          'paramiko==3.4.1',          
          'Jinja2==3.1.4',
          'JPype1==1.4.1'
      ],
      packages=find_packages(),
      )