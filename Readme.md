# Plane Incidents over the Century
### Introduction
Having been on several flights this year got me thinking about recent plane incidents, with one of the major airline manufacturers instantly coming into my mind; including the Sunita Williams space shuttle snafu.

On this, I recently read an article from [The Guardian](https://www.theguardian.com/us-news/2025/mar/01/plane-crash-safety-data) on plane safety data. as quoted from the article:
```
But the numbers suggest 2025 has actually been a relatively safe year to fly – at least in terms of the overall number of accidents". 
```

### Problem Statement
Let's examine this statement, and find out for ourselves whether this is true using our knowledge of data engineering

## Objective
Investigate flight safety over the last century (circa 1920 to 2025). 
We can use this data to answer some interesting questions are plane incidents
1. What's the Year over Year incident levels for the last 100 years?
2. What's the decade over decade incident levels. Have things gotten better?
3. What are some of the causes of incidents as summarized by AI (using gemini LLM in bigquery)?
4. which manufacturer is calpable - investigating incidents classified as manufacturer defect/negligence?
### Data Sources
The [planecrashinfo](https://www.planecrashinfo.com/) website that collates incidents data from various sources.
The data is obtained by clicking on the database section of the website, 
![landing page](public/pc0.png)
after clicking on the database link, the years are displayed as below
![landing page](public/pc1.png)
on clicking on a selected year, the table below is shown
![landing page](public/pc2.png)
after clicking on a specific date, the below details are obtained. 
![landing page](public/pc3.png)

Fortunately, this process is automated in a Kestra flow, and the schema described in the <kbd>data</kbd> section is obtained


#### Data (Schema)
The data contains the fields below: 
- Date
- Time
- Location
- Operator
- Flight
- Route
- AC Type
- Registration
- cn/ln
- Aboard
- Fatalities
- Ground
- Summary

### Technologies
- Docker (containerization)
- Terraform (infrastructure as code)
- Kestra (workflow orchestration)
- Google Cloud Storage (data lake)
- BigQuery (data warehouse)
  - Bigquery ML (summaries classification)
- dbt (data transformation)
- Looker Studio (data visualization)

## Reproducability
<details>
<summary>GCP Setup</summary>

- Follow the GCP instructions in setting up a project

- We set up a service account to aide Kestra/Terraform/Other infrastructure tool in accessing the GCP platform. 
  
- Configure the GCP service account by accessing I&M and Admin -> service accounts -> create service account. Add the required roles (Bigquery Admin, Compute Admin and Storage Admin)

- To get the service account key, click on the dropdown -> manage keys -> create key (choose JSON). This downloads the key to be used in Kestra to setup Bigquery db and Bucket in this instance

</details>

<details>
<summary>Kestra Setup</summary>
Ensure to docker is setup and installed as per your operating system (ensure docker engine is installed). Follow the instructions [here](https://docs.docker.com/engine/install/). 

Go the [kestra website](https://kestra.io/docs/getting-started/quickstart#start-kestra) -> get Started -> goto the commands code. 

```
docker run --pull=always --rm -it -p 8080:8080 --user=root -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp kestra/kestra:latest server local
```

Ensure to run the hello-world command to ensure docker is properly running

```
 sudo docker run hello-world
```

</details>

<details>
<summary>Infrastracture setup with Kestra</summary>

> Instead of using Terraform for this assignment, I preferred using a singular tool for the Infrastracture setup

</details>

## Data Pipeline
![landing page](public/IaC.png)

The steps employed are:
  - Extract (get data from source, in this case the [planecrashinfo](https://www.planecrashinfo.com/) website)
  - Load - convert data from the website into CSVs -> this is then saved in a bucket on Google Cloud Storage -> which is then loaded into an external table
  - Transform - convert into analytics views from the external table. The processes are described below in the [dbt cloud section](#transformation-using-dbt-cloud)



### Data Warehouse
**Google Cloud Storage** - used for storing csv files that have been converted from the orchestration flow script. The CSV files are saved for individual years
![GCS bucket](public/bucket.png)


**Bigquery** - create an external table with data from the bucket. This table is used in DBT to set the staging table, which assigns proper data types to the columns after the data cleaning process. The staging table is then used to create fact tables.

### BigQuery ML
From the schema, the *Summary* column gives a commentary on the (probable) cause of the incident. I use this column to classify the incidents into the options below. These are saved in the *ml_classification* column:
- Manufacturer defect/egligence 
- Operator(ions) error
- Pilot error
- Terrorism
- Indeterminate

The classifications are performed by Googles Gemini LLM, integrated into a Bigquery model, and an update is done to the *ml_classification* column from the adjacent *Summary* column. The code for this process is found [here](Dev_Readme.md)

These classifications are then used in the final dashboard to check on *manufacturer defect/negligence* incidents-count per manufacture.

N.B - the classification process accuracy is > 80%. Using the naive approach of manually checking the summary vs output from the LLM, some summaries have a vague classification. Also the LLM has a bias of classification on mention of a keyword e.g. a commentary mentioning a pilot might wrongly be classified into *pilot error*

Classifications are also backed up into a table outside the DBT models; this is because each classification takes on average ~6s, and with a dataset of ~5k records, this takes approximately *30,000s* to process all the commentaries. The backup table acts as a seed file in dbt when (re)building the models


### Transformation using DBT Cloud
I used dbt cloud which contains the below setup steps:

- Register an account
- Create a project, follow the instructions as guided by dbt
- create a repo in git
- Go to project -> settings -> repository -> attach to the created repo -> copy the _deploy key_
- on the git repo, got to repo settings -> deploy keys -> add deploy key

The link to the dbt repo is [here](https://github.com/dakn2005/dbt_capstone_repo).

The models folder described the schema of the database from external table -> staging table -> facts table -> analytics views ![dbt models, db schema](public/dbt_schema.png)

The repo also contains several [macros](github.com/dakn2005/dbt_capstone_repo/tree/main/macros) which aide in the data transformation process. Of note is the get_plane_manufacturer_name that maps partial named strings into structured manufacturer names e.g. Mc, MD and Mc Douglas are mapped into McDonnell Douglas.



## [Dashboards](https://lookerstudio.google.com/s/h85L32U2D1E)
