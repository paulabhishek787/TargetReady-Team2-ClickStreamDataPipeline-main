# Clickstream Data Pipeline

_Contains code and other configurations required to process clickstream data_

## Usecase
The pipeline read static data, transform, performs dataquality checks and load data to a structured database and draw insights out of the dataset in form of visualizations.

### Source
Source of data for this pipeline is the clickstream and item dataset in the CSV format.

### Destination
The transformed data is finally written to a mySQL table using spark JDBC. Below is the production table:

```
target_ready_prod.clickstream
```

### About the Code

The code reads data from input source diractory and write it into kafka stream. After loading data from kafka topic into a dataframme it performs various cleansing operations (case conversion, elimination NULLs and duplicates, concatenate, split, trim, lowercase and uppercase, dataframe columns) and transformation (joins). It loads the transformed data to a stage table in mySQL, on top of which data quality checks are performed. When the checks pas, the data is finally written to production table for users to consume.




