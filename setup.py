from setuptools import setup
setup(
    name = "pyvsphere",
    version = "0.5.0",
    packages = ['pyvsphere'],
    author = "F-Secure Corporation",
    author_email = "<TBD>",
    description = "pyvsphere is a Python client for the VMware vSphere API",
    license = "Apache License, Version 2.0",
    entry_points = {
        'console_scripts' : [
            'pyvsphere-tool = pyvsphere.vmtool:main',
            ]
        }
)
