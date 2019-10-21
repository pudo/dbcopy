from setuptools import setup

setup(
    name='dbcopy',
    version='0.1.1',
    description="Copy contents of a SQL database to another",
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
        'mssql': [
            'pyodbc',
        ],
    },
    entry_points={
        'console_scripts': [
            'dbcopy = dbcopy.cli:dbcopy'
        ],
    },
)
