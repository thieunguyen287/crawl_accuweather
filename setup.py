import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="example_pkg",
    version="0.0.1",
    author="Thieu Nguyen",
    author_email="thieunguyen287@gmail.com",
    entry_points={'scrapy': ['settings = crawl_accuweather.settings']},
    description="Accuweather crawler",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thieunguyen287/crawl_accuweather",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
