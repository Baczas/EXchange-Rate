# (Weekly) exchange rate report - EXR

Application gather exchange rate data from APIs implemented in Exchange_rate class and update them every day. Data are keept in BigQuery table by default are only 7 days of data.
Data every day are pulled from API, if data are not available in the API take yesterday data as actual once. EXR have two classes **API** and **Exchange_rate**.

## Exchange_rate class
During initialization it takes arguments: 

- table_name: table name from BigQuery in format `<project_id>.<data_set>.<table_name>` 
- credentials: name of service account .json file (with requierd BQ permitons) 
- (optional) currency: main report currency (EUR in default)
- (optional) init_date: base date used in many functions (datetime.today() in default)
- (optional) num_of_days: number of days stored in exchange_rate table (7 in default)


#### Methods

- **set_dates(self, date)** - method that sets the dates for the class based on the given **date** parameter.
  If **date** is a string, it is converted to a datetime object.
   The now and **day_to_del** class variables are also set based on the num_of_days parameter.

- **del_from_BQ(self, source, date)** - method that deletes data from the BigQuery table for the given **source** and **date**.
It take two args:
    - source: source of information name tag ( [list of name tags](#implemented-api-sources) )
    - date: `'YYYY-mm-dd'`

- **fill_day_from_BQ(self, source)** - method that fills a day's worth of data in the BigQuery table for the given **source**.
 The data is copied from the previous day. It take arg: 
    - source: source of information name tag ( [list of name tags](#implemented-api-sources) )

- **restore_table(self, first_day)** - method that restores the BigQuery table with data from a given **first_day**
 for the number of days specified by the **num_of_days** parameter.

- **daily_update(self)** - method that updates the data in the BigQuery table for all implemented sources.

- **ebc_source_update(self)** - method that updates the data in the BigQuery table with data from the European Central Bank (ECB) API.

- **fci_source_update(self)** - method retrieves currency exchange rate data from the Free Currency API and stores it in a Google BigQuery table.

### Implemented APIs
All implemented api functions need to have specific name format: *`"\<source\>_source_update"`*


##### Implemented API sources

- ebc
- fcapi

## API class

Class API have two methods:

**Class `API`**:
- **`count_rate(curr, value, date, table_name, credentials)`** - static method that returns an array of
 calculated values for the given currency `curr`, value `value`, date `date`,
  and table name `table_name` in BigQuery using `credentials`.
- _`validate(curr, value, date)`_ - static method that checks the correctness of the input data format:
 currency `curr` (should be a three letter name), value `value` (should be of type float),
  and date `date` (should be in the YYYY-MM-DD format). If the data format is incorrect,
   an appropriate error message is returned.



#### Table Schema
**`salesmore-371913.recruitment.exchange_rate`** table definition:

| column name    | type      | description|
|----------------|-----------|------------|
| CURRENCY       | STRING    |        secondary currency    |
| CURRENCY_DENOM | STRING    |      main currency      |
| SOURCE         | STRING    |    data source code   |
| TIME_PERIOD    | DATE      |  data creation date    |
| OBS_VALUE      | FLOAT     |     ratio between currencies      |

### Planned Updates 
- Adding support for the National Bank of Poland (NBP) API
