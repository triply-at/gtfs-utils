from setuptools import setup
from pathlib import Path

with open("gtfsfilter/__init__.py") as f:
    for line in f:
        if "__version__" in line:
            version = line.split("=")[1].strip().strip('"').strip("'")
            continue

try:
    this_directory = Path(__file__).absolute().parent
    with open((this_directory / 'requirements.txt'), encoding='utf-8') as f:
        requirements = f.readlines()
    requirements = [line.strip() for line in requirements]
except FileNotFoundError:
    requirements = []

setup(
    name='gtfsfilter',
    version=version,
    url='https://github.com/triply-at/gtfsfilter',
    author='Luis Nachtigall',
    author_email='l.nachtigall@triply.at',
    description='gtfsfilter',
    platforms='any',
    packages=['gtfsfilter'],
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "gtfsfilter = gtfsfilter.__main__:main"
        ]
    }
)
