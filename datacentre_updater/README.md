# Soccerverse Data Updater

This application keeps a MySQL database up to date with the latest data from our game **Soccerverse**. It extracts data from various sources, processes it, and updates the destination database. This updated data will be used for statistics and to replace our existing GraphQL setup.

## Overview

- **Data Extraction**: Retrieves data from a SQLite database and a source MySQL database.
- **Data Transformation**: Processes and transforms the data to match the schema of the destination database.
- **Database Update**: Inserts or updates records in the destination MySQL database with the latest data.
- **Trading Data Update**: Specifically updates trading-related data for clubs, players, and users.
- **Error Handling**: Incorporates comprehensive error handling to ensure robustness.

This application is part of the **Data Centre**, and the next phase involves developing an API using Redis cache to improve performance and scalability.

## Features

- **Automated Updates**: Regularly updates the database to reflect the latest game data.
- **Modular Design**: Utilizes separate updater classes for different entities (clubs, players, users) for maintainability.
- **Graceful Shutdown**: Handles termination signals to allow for clean shutdowns without data corruption.
- **Logging**: Provides detailed logging for monitoring and debugging purposes.

## Usage

The application is intended to run inside a Docker container for ease of deployment.