import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cryptoapis-cmurray", # Replace with your own username
    version="0.0.1",
    author="Christian Murray",
    author_email="cmurray.21@dartmouth.edu",
    description="Python frontends for popular crypto exchanges",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/watersnake1/crypto_apis",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
