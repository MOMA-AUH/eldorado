from setuptools import find_packages, setup

setup(
    name="nanopore_basecaller",
    version="0.0.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    entry_points={"console_scripts": ["eldorado = eldorado.main:app"]},
    python_requires=">=3.10",
    install_requires=["typer", "rich"],
    author="Simon Opstrup Drue",
    author_email="simondrue@gmail.com",
)
