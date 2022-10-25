from setuptools import setup, find_packages

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read()

setup(
    name = 'migrator',
    version = '1.0.0',
    author = 'ynesterov',
    author_email = 'my_email@gmail.com',
    description = ' migration tool',
    py_modules = ['migrator', 'app'],
    packages = find_packages(),
    install_requires = [requirements],
    python_requires='>=3.6',
    entry_points = {
        'console_scripts':['migrator=app:main']
    },
    package_data={"": ["*.txt"]}
)
