# Weather Data Pipeline

## Overview
The weather data pipeline is designed to fetch weather data from the OpenWeatherMap API, transform the data using dbt, and load it into a PostgreSQL database. The pipeline is orchestrated using Dagster, which manages the execution, scheduling, and monitoring of the data pipeline.

### Technologies

#### Dagster
[Dagster](https://daster.io) is a data orchestrator that makes it easy to build, schedule, and monitor data pipelines. It allows you to define pipelines in a Python script and provides a user interface to visualize the pipeline, monitor the execution, and view logs. We choose dagster to orchestrate our data pipeline because it provides a simple way to define and manage the pipeline, and it integrates well with dbt and other data tools. Dagster has less constraints compared to other data orchestration tools like Mage.ai , which makes it easier to use and more flexible for our use case. Anything we can build with python, we can build with dagster.

#### dbt
[dbt](https://getdbt.com) is a transformation workflow tool that enables us to use sql to define, manage, and transform our data. We choose dbt because it allows us to define transformations using sql, which is a language that most data engineers are familiar with. dbt also provides a great way to test the data transformations, which is important to ensure the quality of the data.

## Table of Contents

- [Dagster Pipeline](docs/dagster.md)
- [dbt models](docs/dbt.md)
- [Data Modeling (ERD)](docs/erd.md)
