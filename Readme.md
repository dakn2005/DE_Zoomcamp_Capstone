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
- scare aggregated data source - apis failing to work
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

#### Bigquey ML
```
declare prompt_text STRING;
set prompt_text = 'from the text classify into these options: Manufacturer defect/negligence, Operator error, Pilot error, Terrorism, Indeterminate; do not explain and do not give a null, the answer must be classified into one of the provided categories, if unsure choose Indeterminate: ';

with prompt_tab as (
  select 
    record_id,
    concat(prompt_text, summary) prompt,
    summary
  from `zoomcamp_capstone.stg_incident_data`
  -- where record_id = '2b723d90fed75e84528f7a5f15f932a6'
  limit 10
)
SELECT 
  record_id,
  prompt,
  ml_generate_text_llm_result
FROM
  ML.GENERATE_TEXT(
    MODEL `marine-base-449315-s5.models.gemini_model1`,
    (select * from prompt_tab),
    STRUCT(
      0.1 AS temperature, 100 AS max_output_tokens, 0.5 AS top_p,
      40 AS top_k, TRUE AS flatten_json_output));
```

