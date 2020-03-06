from setuptools import setup, find_packages

# setup
setup(
    name='pyobs-pilar',
    version='0.9',
    description='pyobs component for the Pilar TCS',
    packages=['pyobs_pilar'],
    install_requires=[
        'astropy'
    ]
)
