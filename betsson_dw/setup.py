from setuptools import setup, find_packages

setup(
    name="betsson_dw",
    version="0.1.0",
    description="Betsson Data Warehouse ETL Pipeline",
    author="Manuel Bayona",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'sqlalchemy',
        'psycopg2-binary',
        'python-dotenv',
    ],
    python_requires='>=3.8',
)
