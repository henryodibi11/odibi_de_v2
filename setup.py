
from setuptools import setup, find_packages
setup(
    name="odibi_de_v2",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy",
        "fastavro",
        "fsspec",
        "pyarrow",
        "azure-storage-blob",
        "pyspark",
        "pyodbc",
        "delta-spark",
        "iapws",
        "nbformat",
        "ipywidgets"
        "pytest",
        "black",
        "mypy"
    ],
    author="Henry Odibi",
    author_email="odibiengineering@gmail.com",
    description="A modular and scalable data engineering framework using Pandas, Spark, and Azure.",
    long_description="See full documentation at https://github.com/henryodibi11/odibi_de_v2",
    long_description_content_type="text/markdown",
    url="https://github.com/henryodibi11/odibi_de_v2",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)