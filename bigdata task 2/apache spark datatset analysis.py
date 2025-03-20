# Apache Spark Dataset Analysis

This project demonstrates how to use Apache Spark to analyze a large dataset, performing filtering, grouping, and aggregations.

## Prerequisites

* Apache Spark (version 3.0 or later recommended)
* Python 3.x
* `pyspark` library

## Installation

1.  Clone the repository:

    ```bash
    git clone [repository_url]
    cd dataset-analysis
    ```

2.  Create a virtual environment (recommended):

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Linux/macOS
    venv\Scripts\activate  # On Windows
    ```

3.  Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  Place your dataset file (e.g., `your_dataset.csv`) in the `data/` directory.

2.  Modify the `src/analyze_data.py` script to match your dataset's schema and desired analysis operations (column names, filtering conditions, aggregations, etc.).

3.  Run the script:

    ```bash
    python src/analyze_data.py data/your_dataset.csv
    ```

4.  The analysis results will be saved in the `output/` directory (e.g., `results.parquet`).

## Example Analysis (in `src/analyze_data.py`)

* Filtering records based on a specific column value.
* Grouping records by a categorical column and calculating aggregate statistics (count, average, sum).
* Ordering the aggregated results.
* Saving the results to parquet format.
* Adding a new column based on existing columns.

## Contributing

Feel free to contribute to this project by submitting pull requests.

## License

[Your License (e.g., MIT License)]
