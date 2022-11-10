from setuptools import setup, find_packages

install_requires = [
        'nltk',
        'dill',
    ]

print(install_requires)

setuptools.setup(
    name='hakuin',
    version='0.0.1',
    install_requires=install_requires,
    packages=find_packages(),
    python_requires='>=3'
)
