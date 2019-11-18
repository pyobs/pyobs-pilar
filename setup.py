from setuptools import setup, find_packages

# setup
setup(
    name='pyobs-pilar',
    version='0.8',
    description='pyobs component for the Pilar TCS',
    packages=['pyobs_pilar'],
    install_requires=[
        'astropy',
        'pyobs-core'
    ]
)
