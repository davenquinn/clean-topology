from setuptools import setup

setup(
    name='clean_topology',
    version='0.1',
    py_modules=['clean_topology'],
    install_requires=[
        'click',
        'sqlalchemy',
        'psycopg2'],
    entry_points='''
        [console_scripts]
        clean-topology=clean_topology:cli
    ''')
