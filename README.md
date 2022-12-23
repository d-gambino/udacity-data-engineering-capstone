# I94 Immigration Data Pipeline
## Data Engineering Capstone Project
### Project Summary
The objective of this project is to create an ETL pipeline for I94 immigration, global temperatures and US demographics datasets to form an analytics database on immigration events over time. The resulting database will enable analysis on externalities within non-US countries as well as US states which could deliver insight on some of the less obvious factors surrounding immigration to the US.

#### The project follows the following steps:

1. Scope the Project and Gather Data
2. Explore and Assess the Data
3. Define the Data Model
4. Run ETL to Model the Data
5. Complete Project Write Up


### Step 1: Scope the Project and Gather Data
#### Scope
Spark will be used to load the data into dataframes, clean said dataframes, and execute our ETL pipeline to create our final data model. Along the way, we will be using tools like Pandas and Seaborn to help visualize our data and sniff out missing values, problematic datatypes, and other potential obstacles.

Once data is sufficiently cleaned, it will be written to parquet files and stored in an output folder.


##### Description of our core data
Our data comes from the US National Tourism and Trade Office. Historically, all foreign visitors coming into the USA via air or sea were subject to an examination by the Customs Border Protection (CBP) officer after which they would be issued either a passport admission stamp or a small white card called the I-94 form. This form contains precisely the data which we are basing our data model on.

Immigration data is in parquet format (converted from SAS data using a third-party Spark package - spark-sas7bdat).
