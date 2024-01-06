# Python Database Interaction and Analysis

This project includes a set of scripts designed to interact with a PostgreSQL database, generate logs, and analyze the data within the logs. The main components are the `app.py`, `interact.py`, `analyse.py`, and `initiate.py` scripts, each handling different parts of the database interaction and analysis.

![GitHub](https://img.shields.io/github/license/vitorodesouzaUma/analysing-sql-database)
![GitHub last commit](https://img.shields.io/github/last-commit/vitorodesouzaUma/analysing-sql-database)

## Getting Started

These instructions will guide you through setting up and running the project on your local machine for development and testing purposes.

### Prerequisites

Before running this project, you will need:

- Python installed on your system
- Access to a PostgreSQL database with permission to read and write data
- Necessary Python packages installed:
    - pandas
    - matplotlib
    - psycopg2
    - Any other packages used in the scripts described in requirements.txt

### Installation

1. Clone the repository to your local machine or download the zip and extract it.
2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```
3. Create a `config.json` file with your database configuration details based on `config_example.json`

### Usage

To run the application, use the following command from the terminal:

```sh
python app.py [options]
```

#### Options include:

* -i or --init: Initiate the project by creating a database, tables, and inserting initial data.
* --interact: Interact ramdonly with the database to generate logs.
* -a or --analyze: Analyze the generated logs and produce a report.

### Scripts Overview
app.py: The main driver script that parses arguments and calls the respective functionality based on user input.
analyse.py: Contains functionality to analyze the generated logs from the database and produce a report, including a bar chart visualization of interaction counts.
interact.py: Interacts with the database to perform various operations and generate logs.
initiate.py: Handles the initial setup of the database, including creating the database, tables, and necessary functions and triggers.

### Contributing
If you'd like to contribute to this project, please fork the repository and create a pull request with your features or fixes.

### License
This project is licensed under the MIT License - see the LICENSE.md file for details.