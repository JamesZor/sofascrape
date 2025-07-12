from setuptools import find_packages, setup

setup(
    name="sofascrape-package",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "selenium>=4.0.0",
        "hydra-core>=1.3.0",
        "omegaconf>=2.3.0",
        "requests>=2.28.0",
    ],
    package_data={
        "sofascrape": ["conf/**/*.yaml"],
    },
    include_package_data=True,
    author="James",
    description="Handle the interaction with the sofa api.",
)
