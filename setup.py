from setuptools import find_packages, setup

setup(
    name="weather_data_pipeline",
    packages=find_packages(exclude=["weather_data_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-postgres",
        "dagster-slack",
        "pandas",
        "dbt-postgres",
        "psycopg2",
        "ratelimit",
        "tenacity",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
