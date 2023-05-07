from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'NDF finance tools'
LONG_DESCRIPTION = 'This is a finance package to help download and calculate NDF (Non-Deliverable Forward) information'

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="ndfutil",
    version=VERSION,
    author="Cleiton Souza",
    author_email="<cleitonsouza01@gmail.com>",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    url='https://github.com/cleitonsouza01/ndfutil',
    packages=find_packages(),
    install_requires=['holidays~=0.20',
         'joblib~=1.2.0',
         'requests~=2.28.2',
         'loguru~=0.6.0',
         'pandas~=1.5.3',
         'xlrd~=2.0.1',
         'Scrapy~=2.8.0',
         'numerize~=0.12'],  # add any additional packages that

    keywords=['python', 'finance', 'ndf', 'ndfutil', 'Non Deliverable Forward'],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
    ]
)
