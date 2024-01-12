# Instrument Calculator Solution Overview

The Instrument Calculator is a PySpark application designed to calculate financial metrics for different instruments. Below is a guide on how to run, test, and understand the important design decisions made during the development of this solution.

## Running the Solution

### Prerequisites:

- Python 3.9 or higher
- Poetry (for dependency management)
    ```curl -sSL https://install.python-poetry.org | python3 -```

### Steps:

1. **Clone the Repository:**

   ```
   git clone https://github.com/Taabu/wipro_ubs_coding_task.git
   cd wipro_ubs_coding_task
   ```

2. **Install Dependencies with Poetry:**

    ```
    poetry install
    ```

3. **Run:**

    ```
    poetry run python main.py
    ```

4. **Test:**

    ```
    poetry run pytest
    ```


### Performance Considerations

Given the requirement to handle large datasets efficiently, the project utilizes PySpark to leverage distributed processing capabilities. This allows the solution to scale effectively and process gigabytes of data without encountering memory errors.

The repartition method is strategically used to manage the number of partitions based on the size of the data and the cluster configuration.

The broadcast function is employed for smaller DataFrames to optimize join operations by reducing data shuffling.

Operations involving shuffling, such as groupBy, are minimized where possible to avoid the associated computational cost.

The Spark cluster configuration is adjusted within the code using the config method to align with available resources and dataset characteristics.

