from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name='dbcopy',
    version='0.1.3',
    description="Copy contents of a SQL database to another",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/pudo/dbcopy',
    author='OCCRP',
    license='MIT',
    packages=['dbcopy'],
    py_modules=['dbcopy'],
    include_package_data=True,
    install_requires=[
        'sqlalchemy',
        'stringcase',
        'normality',
        'click',
        'tqdm',
    ],
    extras_require={
        'postgres': [
            'psycopg2-binary',
        ],
        'postgressql': [
            'psycopg2-binary',
        ],
        'mssql': [
            'pyodbc',
        ],
        'mysql': [
            'PyMySQL',
        ]
    },
    entry_points={
        'console_scripts': [
            'dbcopy = dbcopy.cli:dbcopy'
        ],
    },
)
