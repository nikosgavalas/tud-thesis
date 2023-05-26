import setuptools
classifiers = [
    "Programming Language :: Python :: 3",
]
setuptools.setup(
    name="kevo",
    version="3.0.0",
    author="Nikos Gavalas",
    description="Key-value store with 3 backend engines",
    classifiers=classifiers,
    keywords=["key-value", "store"],
    packages=setuptools.find_packages(),
    install_requires=[
        'sortedcontainers >= 2.4.0, < 3',
        'bitarray >= 2.6.2, < 3',
        'mmh3 >= 3.0.0, < 4',
        'minio >= 7.1.14, < 8',
    ],
    python_requires='>=3.11',
)
