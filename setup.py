from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='pybee',
    version='0.1.0',
    description='Lightweight SQL, SSH, and file operations for Hive/Spark environments',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Zahoor Ahmed',
    author_email='infinitezahoor@gmail.com',
    url='https://github.com/Zahoor-Ahmed/pybee',
    license='MIT',
    python_requires='>=3.9',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "paramiko==3.5.1",
        "bcrypt>=3.2,<4.0",
        "cryptography==41.0.7",
        "pynacl>=1.5,<2.0",
        "pandas>=2.2.3,<3.0.0",
        "ipython==8.12.3",
        "pygame>=2.6.1,<3.0.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
)