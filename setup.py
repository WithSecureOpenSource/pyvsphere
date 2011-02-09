from setuptools import setup
setup(
    name = "pyvsphere",
    version = "0.1",
    packages = ['pyvsphere'],
    scripts = ['vmtool.py'],
    author = "Gergely Erdelyi",
    author_email = "ext-gergely.erdelyi@f-secure.com",
    description = "pyvsphere is a Python client for the VMware vSphere API",
    license = "FSC INTERNAL"
)
