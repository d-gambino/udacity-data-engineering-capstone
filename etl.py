import os
import configparser
import datetime as dt
from itertools import chain
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, create_map, lit, monotonically_increasing_id

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ["PYSPARK_PYTHON"] = "python"
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

spark = SparkSession.builder\
                    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk:1.12.369")\
                    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
                    .enableHiveSupport()\
                    .getOrCreate()


def load_dataframes(immigration_fp, temperature_fp, demographics_fp, airport_fp):
    """
    Loads files into respective Spark dataframes for staging
    :param immigration_fp: filepath which contains the immigration parquet data
    :param temperature_fp: filepath which contains the temperature CSV data
    :param demographics_fp: filepath which contains the demographics CSV data
    :param airport_fp: filepath which contains the US airports CSV data
    :return: all respective Spark dataframes
    """

    immigration_file = immigration_fp
    df_immigration = spark.read.parquet(immigration_file)

    temperature_file = temperature_fp
    df_temperature = spark.read.csv(temperature_file, header=True, inferSchema=True)

    demographics_file = demographics_fp
    df_demographics = spark.read.csv(demographics_file, inferSchema=True, header=True, sep=';')

    airport_file = airport_fp
    df_airport_codes = spark.read.csv(airport_file, header=True, inferSchema=True)
    df_airport_codes = df_airport_codes.filter(df_airport_codes.iso_country == 'US')

    return df_immigration, df_temperature, df_demographics, df_airport_codes


def fetch_sas_key_values(labels_fp, df_immigration):
    """
    Maps key-value pairs from the SAS Labels Description file to I-94 codes and inserts them as new columns
    """

    # Retrieving key-value pairs from SAS Labels Descriptions file
    print('Creating label fields based on I-94 IDs...')
    with open(labels_fp) as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')

    def code_mapper(file, idx):
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic

    i94cit_res = code_mapper(f_content, "i94cntyl")
    i94mode = code_mapper(f_content, "i94model")
    i94addr = code_mapper(f_content, "i94addrl")
    i94visa = {'1': 'Business',
               '2': 'Pleasure',
               '3': 'Student'}

    # Converting id fields to integers for value mapping
    df_immigration = df_immigration.withColumn("cicid", df_immigration.cicid.cast("int")) \
        .withColumn("i94yr", df_immigration.i94yr.cast("int")) \
        .withColumn("i94mon", df_immigration.i94mon.cast("int")) \
        .withColumn("i94cit", df_immigration.i94cit.cast("int")) \
        .withColumn("i94res", df_immigration.i94res.cast("int")) \
        .withColumn("i94mode", df_immigration.i94mode.cast("int")) \
        .withColumn("i94bir", df_immigration.i94bir.cast("int")) \
        .withColumn("i94visa", df_immigration.i94visa.cast("int"))

    # Creating map literals to create value columns based on key mappings
    mapping_i94cit_res = create_map([lit(x) for x in chain(*i94cit_res.items())])
    mapping_i94mode = create_map([lit(x) for x in chain(*i94mode.items())])
    mapping_i94addr = create_map([lit(x) for x in chain(*i94addr.items())])
    mapping_i94visa = create_map([lit(x) for x in chain(*i94visa.items())])

    # Adding columns to dataframe based on I-94 codes and their mappings
    df_immigration = df_immigration.withColumn("i94cit_value", mapping_i94cit_res.getItem(col("i94cit"))) \
        .withColumn("i94res_value", mapping_i94cit_res.getItem(col("i94res"))) \
        .withColumn("i94mode_value", mapping_i94mode.getItem(col("i94mode"))) \
        .withColumn("i94addr_value", mapping_i94addr.getItem(col("i94addr"))) \
        .withColumn("i94visa_value", mapping_i94visa.getItem(col("i94visa")))

    return df_immigration


def clean_immigration_data(df_immigration):
    """
    Inputs: I-94 immigration Spark dataframe

    This function takes in the preloaded immigration dataframe and performs a few data cleaning tasks...
    1. Drops incomplete fields
    2. Drops any rows with duplicate `cicid` values
    3. Converts SAS dates to datetype
    """

    print('Cleaning (1/3): Converting SAS dates to date type...')
    # Create UDF to convert SAS dates to date type
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    df_immigration = df_immigration.withColumn("arrdate", get_date(df_immigration.arrdate)) \
        .withColumn("depdate", get_date(df_immigration.depdate))

    print('Cleaning (2/3): Dropping fields with mostly incomplete data...')
    # Dropping fields with mostly incomplete data
    drop_cols = ['occup', 'entdepu', 'insnum']
    df_immigration = df_immigration.drop(*drop_cols)

    print('Cleaning (3/3): Dropping fields with mostly incomplete data...')
    # Dropping rows with duplicate cicid values
    df_immigration = df_immigration.dropDuplicates(['cicid'])
    cleaned_df_immigration = df_immigration.dropna(how='all', subset=['cicid'])

    print('Cleaning: Done!')

    return cleaned_df_immigration


def clean_temperature_data(df_temperature):
    """
    Inputs: Country temperatures Spark dataframe

    This function takes in the preloaded temperatures dataframe and drops (a) duplicate rows,
    and (b) rows with missing average temperatures.
    """
    df_temperature = df_temperature.dropDuplicates()
    cleaned_df_temperature = df_temperature.na.drop(subset=["AverageTemperature"])

    print('Done!')
    return cleaned_df_temperature


def process_tables(df_temperature, df_airport_codes, df_demographics, df_immigration, output_path):

    df_immigration.createOrReplaceTempView("df_immigration")
    calendar_dim = spark.sql("""
                                select distinct
                                    arrdate as date,
                                    year(arrdate) as year,
                                    month(arrdate) as month,
                                    weekofyear(arrdate) as week,
                                    dayofyear(arrdate) as day,
                                    dayofweek(arrdate) as weekday
                                from df_immigration
                                """)

    df_country = df_temperature.select(col("Country")).distinct()
    df_temperature.createOrReplaceTempView("df_temperature")
    df_country.createOrReplaceTempView("df_country")
    country_dim = spark.sql("""
                                select
                                    row_number() over (ORDER BY "c.Country") as country_id,
                                    c.Country as country_name,
                                    min(t.AverageTemperature) as country_avg_temp_min,
                                    max(t.AverageTemperature) as country_avg_temp_max
                                from df_country c
                                left join df_temperature t on c.Country = t.Country
                                group by c.Country

                                """)

    df_airport_codes.createOrReplaceTempView("df_airport_codes")
    us_airport_dim = spark.sql("""
                                select
                                    row_number() over (order by "iata_code") as airport_id,
                                    iata_code as airport_code,
                                    type as airport_type,
                                    name as airport_name,
                                    continent as continent_code,
                                    case 
                                        when continent = 'NA' then 'North America'
                                        when continent = 'AF' then 'Africa'
                                        when continent = 'AN' then 'Antarctica'
                                        when continent = 'AS' then 'Asia'
                                        when continent = 'EU' then 'Europe'
                                        when continent = 'OC' then 'Oceania'
                                        when continent = 'SA' then 'South America'
                                    end as continent_name,
                                    iso_country as country_code,
                                    right(iso_region,2) as state_code,
                                    municipality
                                from df_airport_codes
                                """)

    df_demographics.createOrReplaceTempView("df_demographics")
    us_demographics_dim = spark.sql("""
                                    select
                                        row_number() over (order by `State Code`) as state_id,
                                        `State Code` as state_code,
                                        State as state_name,
                                        concat(cast(min(`Median Age`) as string)," - ",cast(max(`Median Age`) as string)) as median_age_range,
                                        sum(`Total Population`) as population,
                                        sum(`Number of Veterans`) as veteran_pop,
                                        sum(`Foreign-born`) as foreign_born_pop,
                                        concat(cast(min(`Average Household Size`) as string)," - ",cast(max(`Average Household Size`) as string)) as avg_household_size_range,
                                        sum(Count) as count
                                    from df_demographics
                                    group by 
                                        `State Code`,
                                        State
                                    """)

    us_airport_dim.createOrReplaceTempView("us_airport_dim")
    us_demographics_dim.createOrReplaceTempView("us_demographics_dim")
    country_dim.createOrReplaceTempView("country_dim")
    df_immigration.createOrReplaceTempView("df_immigration")
    immigration_fact = spark.sql("""
                                select 
                                    birth.country_id as birth_country_id,
                                    res.country_id as res_country_id,
                                    p.airport_id,
                                    d.state_id,
                                    arrdate as arrival_date,
                                    depdate as departure_date,
                                    dtadfile as created_date,
                                    dtaddto as admission_date,
                                    i94visa as visa_type_code,
                                    i94visa_value as visa_type_desc,
                                    visapost as visa_post,
                                    i94mode as arrival_mode_code,
                                    i94mode_value as arrival_mode_desc,
                                    i94bir as age,
                                    entdepa as arrival_flag,
                                    entdepd as departure_flag,
                                    matflag as match_flag,
                                    biryear as birth_year,
                                    admnum as admission_num,
                                    fltno as flight_num,
                                    airline airline_code
                                from df_immigration i
                                left join country_dim birth on lower(i.i94cit_value)=lower(birth.country_name)
                                left join country_dim res on lower(i.i94cit_value)=lower(res.country_name)
                                left join us_airport_dim p on i.i94port = p.airport_code
                                left join us_demographics_dim d on i.i94addr = d.state_code
                                """)

    immigration_fact = immigration_fact.withColumn("record_id", monotonically_increasing_id())

    print('(1/5) Creating calendar_dim...')
    calendar_dim.write.parquet(output_path + "calendar_dim", partitionBy=['year', 'month', 'week'], mode="overwrite")

    print('(2/5) Creating us_demographics_dim...')
    us_demographics_dim.write.parquet(output_path + "us_demographics_dim", partitionBy='state_code', mode="overwrite")

    print('(3/5) Creating us_airport_dim...')
    us_airport_dim.write.parquet(output_path + "us_airport_dim", mode="overwrite")

    print('(4/5) Creating country_dim...')
    country_dim.write.parquet(output_path + "country_dim", mode="overwrite")

    print('(5/5) Creating immigration_fact...')
    immigration_fact.write.parquet(output_path + "immigration_fact", mode="overwrite")

    print('Done!')


def main():

    labels_fp = "./I94_SAS_Labels_Descriptions.SAS"
    immigration_fp = os.path.join(SOURCE_S3_BUCKET + "sas_data/*.parquet")
    temperature_fp = os.path.join(SOURCE_S3_BUCKET + "temperature_data/GlobalLandTemperaturesByCity.csv")
    demographics_fp = os.path.join(SOURCE_S3_BUCKET + "us-cities-demographics.csv")
    airport_fp = os.path.join(SOURCE_S3_BUCKET + "airport-codes_csv.csv")
    output_path = DEST_S3_BUCKET

    df_immigration, df_temperature, df_demographics, df_airport_codes = load_dataframes(immigration_fp, temperature_fp,
                                                                                        demographics_fp, airport_fp)
    df_immigration = fetch_sas_key_values(labels_fp, df_immigration)
    df_immigration = clean_immigration_data(df_immigration)
    df_temperature = clean_temperature_data(df_temperature)
    process_tables(df_temperature, df_airport_codes, df_demographics, df_immigration, output_path)


if __name__ == "__main__":
    main()
