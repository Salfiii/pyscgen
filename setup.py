import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyscgen",
    version="0.0.1",
    author="",
    author_email="",
    description="Python AVRO Schema generator which can use multiples example JSONs as input to infer the Schema.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    url="",
    setup_requires=["pytest-runner"],
    install_requires=[
    ],
    dependency_links=[
        "https://pypi.python.org/simple/"
    ],
    tests_require=["pytest"],
    packages=setuptools.find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)