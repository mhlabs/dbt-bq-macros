# dbt-bq-macros

This repo contains dbt macros that can be used in bigquery. 

**[Macros](#macros)**

- [log_to_scd](#log_to_scd)
- [log_to_latest](#log_to_latest)
- [stg_dedup](#stg_dedup)

## Macros

### log_to_scd ([source](macros/log_to_scd.sql))

This macro transform a log table to a slowly changing dimension table of type 2. The undelying table is assumed to contain log events where not all of them contains actual updaes to the data, metadata columns not of interest is therefore excluded from comparing whether the data changed or not. The resulting table will have rows with a valid_from and valid_to column as well as a version column.

### Macro Arguments
- `table_`: Name of the table or CTE of the log table from the context where this macro is used
- `id`: Identifier of the entity
- `event_id`: Unqiue identifier for each row
- `updated_at`: Timestamp column indicated when this event happened 
- `excluded_cols`: Columns to exclude while evaluating whether any data has changed.

### Resulting table

- All columns except the excluding columns plus:
- `valid_from`: From the timestamp the data was updated
- `valid_to`: Until the data is changed
- `version`: Incrementing version for each id 


### Example

Consider the following table where we say that event_id, updated_at and processing_time should be excluded. We are only interested to know if the price changes to a product.

| id| event_id| price|    updated_at   | processing_time  |  
|-- |:-------:|-----:| ---------------:|  --------------: |
| 1 |  1      |  10  | 2021-01-01 00:00| 2021-01-01 00:00 |
| 1 |  2      |  10  | 2021-01-02 00:00| 2021-01-02 00:00 |
| 1 |  3      |  11  | 2021-01-03 00:00| 2021-01-03 00:00 |
| 1 |  4      |  10  | 2021-01-04 00:00| 2021-01-04 00:00 |
| 2 |  5      |  20  | 2021-01-05 00:00| 2021-01-05 00:00 |

### Calling the macro with params:

log_to_scd(`table_`= log_table, `id`='id', `event_id`='event_id',`updated_at`='updated_at',`excluded_cols`='event_id, updated_at, processing_time')

Outputs:

| id| version | price|    valid_from   |     valid_to     |  
|-- |:-------:|-----:| ---------------:|  --------------: |
| 1 |  1      |  10  | 2021-01-01 00:00| 2021-01-03 00:00 |
| 1 |  2      |  11  | 2021-01-03 00:00| 2021-01-04 00:00 |
| 1 |  3      |  10  | 2021-01-04 00:00| 2071-01-04 00:00 |
| 2 |  1      |  20  | 2021-01-05 00:00| 2071-01-05 00:00 |


### log_to_latest ([source](macros/log_to_latest.sql))

This macro transform a log table to a latest table, showing only the latest record for each id.

### Macro Arguments
- `table_`: Name of the table or CTE of the log table from the context where this macro is used
- `id`: Natural key of the table
- `updated_at`: Timestamp column indicated when this event happened 

### Resulting table

- One row for each id

### Example

Log_table:

| id| event_id| price|    updated_at   |  
|-- |:-------:|-----:| ---------------:|
| 1 |  1      |  10  | 2021-01-01 00:00|
| 1 |  2      |  10  | 2021-01-02 00:00|
| 1 |  3      |  11  | 2021-01-03 00:00|
| 1 |  4      |  10  | 2021-01-04 00:00|
| 2 |  5      |  20  | 2021-01-05 00:00|

### Calling the macro with params:

log_to_latest(`table_`= log_table, `id`='id',`updated_at`='updated_at')

| id| event_id| price|    updated_at   |  
|-- |:-------:|-----:| ---------------:|
| 1 |  1      |  10  | 2021-01-01 00:00|
| 1 |  2      |  10  | 2021-01-02 00:00|
| 1 |  3      |  11  | 2021-01-03 00:00|
| 1 |  4      |  10  | 2021-01-04 00:00|
| 2 |  5      |  20  | 2021-01-05 00:00|


### stg_dedup ([source](macros/stg_dedup.sql))

This macro transform a log table that can contain duplicates to a deduplicated table with an added primary key. 

### Macro Arguments
- `table_`: Name of the table or CTE of the log table from the context where this macro is used
- `id`: Natural key of the table
- `pk`: Name of the primary key that will be added
- `updated_at`: Timestamp column used for creating the primary key.

### Resulting table

- Duplicates removed
- Primary key added as a surrogate key with id and updated at field

### Example

Log table:

| id| supplier | price|    updated_at  |  
|-- |:-------:|-----:| ---------------:|
| 1 |  1      |  10  | 2021-01-01 00:00|
| 1 |  2      |  10  | 2021-01-02 00:00|
| 1 |  3      |  11  | 2021-01-03 00:00|
| 1 |  3      |  11  | 2021-01-03 00:00|
| 2 |  5      |  20  | 2021-01-05 00:00|


### Calling the macro with params:

stg_dedup(`id`='id', `pk`='product_event_id',`updated_at`='updated_at')

Outputs:

| id| supplier | price|    updated_at   |  product_event_id   |   
|-- |:-------:|-----: | ---------------:| --------------------|
| 1 |  1      |  10   | 2021-01-01 00:00| 1                   |
| 1 |  2      |  10   | 2021-01-02 00:00| 2                   |
| 1 |  3      |  11   | 2021-01-03 00:00| 3                   |
| 2 |  5      |  20   | 2021-01-05 00:00| 4                   |