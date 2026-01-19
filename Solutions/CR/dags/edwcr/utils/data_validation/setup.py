import setuptools

setuptools.setup(
    name='data validation',
    version='1.0',
    install_requires=[
        'google-cloud-bigquery',
        'Mako',
        'pyyaml',
        'google-cloud-storage'
    ],
    packages=setuptools.find_packages(),
)