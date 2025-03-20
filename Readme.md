#### Dataset 
```
CREATE OR REPLACE external TABLE `marine-base-449315-s5.zoomcamp_capstone.external_data_2` (
  `date` date,
  `time` string,
  location string,
  operator string,
  flight string,
  route string,
  actype string,
  registration string,
  cn_ln string,
  aboard string, 
  fatalities string, 
  ground string, 
  summary string
)
OPTIONS (
  format = 'csv',
  skip_leading_rows = 1,
  -- field_delimiter = '|',
  allow_quoted_newlines = true,
  uris = [
    'gs://marine-base-449315-s5-kestra-bucket-4/2002.csv'
    ]
);

select * from `zoomcamp_capstone.external_data_2`
;
```

#### Challenges
- obtaining data - the actual data were links after accessing the landing page for the specific year
- String process - free text has special characters (e.g. apostrophes, hashed) 
  - preprocessed this at the dataframe stage before saving as CSV
  - added allow_quoted_newlines = true for CSV multi-line text

### DBT 
DBT models and links are in this repository

### Bigquery LLM Classifier
Classification options
- Manufacturer negligence 
- Operator(ions) error
- Pilot error
- Terrorism
- Indeterminate

