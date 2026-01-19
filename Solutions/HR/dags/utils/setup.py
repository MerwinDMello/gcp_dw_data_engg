from setuptools import setup, find_packages
setup(name='Dependencies',
      description='Dependencies',
      install_requires=[
          'pandas==1.5.1',
          'pandas-gbq==0.17.9',
          'JayDeBeApi==1.2.3',
          'google-cloud-secret-manager==1.0.2',
          'SQLAlchemy==1.4.27',
          'JPype1==1.4.1',
          'pendulum==2.1.2'          
          
      ],
      packages=find_packages(),
      )