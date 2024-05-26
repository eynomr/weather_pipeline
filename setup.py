from setuptools import find_packages, setup

setup(
    name="weather_data_pipeline",
    packages=find_packages(exclude=["weather_data_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "dbt-postgres",
        "dagster-postgres",
        "psycopg2",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
