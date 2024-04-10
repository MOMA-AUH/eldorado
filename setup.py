from setuptools import find_packages, setup

setup(
    name="eldorado",
    version="0.0.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    entry_points={"console_scripts": ["eldorado = eldorado.main:app"]},
    test_suite="tests",
    python_requires=">=3.10",
    install_requires=["typer", "rich", "pod5"],
    author="Simon Opstrup Drue",
    author_email="simondrue@gmail.com",
)
