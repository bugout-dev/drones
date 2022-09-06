from setuptools import find_packages, setup

from drones.version import DRONES_VERSION

long_description = ""
with open("README.md") as ifp:
    long_description = ifp.read()

setup(
    name="bugout-drones",
    version=DRONES_VERSION,
    author="Bugout.dev",
    author_email="engineering@bugout.dev",
    description="Drones and automatization",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bugout-dev/drones",
    platforms="all",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development :: Libraries",
    ],
    python_requires=">=3.6",
    packages=find_packages(),
    package_data={"bugout": ["py.typed"]},
    zip_safe=False,
    install_requires=[
        "boto3",
        "fastapi",
        "redis",
        "pydantic",
        "sendgrid",
        "bugout-spire>=0.4.2",
        "sqlalchemy",
        "tqdm",
    ],
    extras_require={
        "dev": [
            "black",
            "mypy",
        ],
        "distribute": ["setuptools", "twine", "wheel"],
    },
    entry_points={"console_scripts": ["drones=drones.cli:main"]},
)
