# I94 Immigration Data Pipeline: Data Engineering Capstone Project
### Project Summary
The objective of this project is to create an ETL pipeline for I94 immigration, global temperatures and US demographics datasets to form an analytics database on immigration events over time. The resulting database will enable analysis on externalities within non-US countries as well as US states which could deliver insight on some of the less obvious factors surrounding immigration to the US.

#### The project follows the following steps:

1. Scope the Project and Gather Data
2. Explore and Assess the Data
3. Define the Data Model
4. Run ETL to Model the Data
5. Complete Project Write Up
  
  
## Step 1: Scope the Project and Gather Data
#### Scope
Spark will be used to load the data into dataframes, clean said dataframes, and execute our ETL pipeline to create our final data model. Along the way, we will be using tools like Pandas and Seaborn to help visualize our data and sniff out missing values, problematic datatypes, and other potential obstacles.  

Once data is sufficiently cleaned, it will be written to parquet files and stored in an output folder.  


#### Description of our core data
Our data comes from the US National Tourism and Trade Office. Historically, all foreign visitors coming into the USA via air or sea were subject to an examination by the Customs Border Protection (CBP) officer after which they would be issued either a passport admission stamp or a small white card called the I-94 form. This form contains precisely the data which we are basing our data model on.  

Immigration data is in parquet format (converted from SAS data using a third-party Spark package - spark-sas7bdat).   

## Step 3: Define the Data Model
View ERD.png or refer to Submission.ipynb for the visual representation of our data model. 

#### The Fact Table
Our immigration fact table was left at its most granular level possible, allowing us to have flexibility should we want to create a proper data warehouse further down the line with different levels of aggregation. As such, our data model can drill down to the date-time level for each individual immigrant. Tables with values at city-level granularity were aggregated to state-level or higher to enable joins to our fact table and avoid exploding our data unnecessarily.

#### The Dimension Tables
Our dimension tables include demographic information for each US city, average temperatures for various countries, and descriptive airport information, all of which can be joined to our fact table's foreign keys. We also have a calendar table (derived from the SAS dates in our immigration data) which will allow us to filter and group by various time dimensions with ease.

#### Why?
The purpose of this data model is to enable users to analyse US immigration data with added context coming from the average temperature of resident countries at the time of departure as well as demographic information of the US states that individuals are immigrating to.

## Step 4: Run Pipelines to Model the Data, perform data quality checks
Refer to Submission.ipynb for details on this step

