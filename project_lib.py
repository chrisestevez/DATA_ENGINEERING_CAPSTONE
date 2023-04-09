import pyspark.sql.functions as F
import logging
import glob
import os
import re
from itertools import chain
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame as SparkDataFrame
import time
import pandas as pd
import requests
import datetime
from typing import Dict, Optional, Union

logging.basicConfig(filename = './data/log/file.log',
                    level = logging.WARNING,
                    format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')

class Utilities:
    """Class used to extract data & check that project is setup correctly.
    """
    
    @staticmethod
    def byte_size(byte:int, unit:str='mb', precision:int=5)-> float:
        """Converts bytes to different unit.

        Args:
            byte (int): bytes
            unit (str, optional): Conversion. Defaults to 'mb'.
            precision (int, optional): float precission. Defaults to 5.

        Raises:
            ValueError: Unit must be kb, mb, or gb

        Returns:
            float: Conversion number
        """
        exponents_map = {'bytes': 0, 'kb': 1, 'mb': 2, 'gb': 3}
        if unit not in exponents_map:
            raise ValueError("Must select from \
            ['bytes', 'kb', 'mb', 'gb']")
        else:
            size = byte / 1024 ** exponents_map[unit]
            return round(size, precision)


    def _urljoin(self, *parts):
        """Helper function used to combine url

        Returns:
            str: Combined string
        """
        return ''.join([p for p in parts])


    def noaa_api(self,
            base_url:str="https://www.ncei.noaa.gov/access/services/data/v1?",
            dataset:str="daily-summaries",
            data_type:str="TMAX,TMIN,TAVG,AWND,WT01,PRCP,SNOW,AWND",
            stations:str="USW00094728", start_date:str="",
            end_date:str="")->pd.DataFrame:
        """Function used to extract data from the NOAA API.

        Args:
            base_url (str, optional): Main url. Defaults to \
                "https://www.ncei.noaa.gov/access/services/data/v1?".
            dataset (str, optional): Report. Defaults to "daily-summaries".
            data_type (str, optional): Measures. \
                Defaults to "TMAX,TMIN,TAVG,AWND,WT01,PRCP,SNOW,AWND".
            stations (str, optional): Weather station. Defaults to \
                "USW00094728".
            start_date (str, optional): Report initial date. Defaults to "".
            end_date (str, optional): Report end date. Defaults to "".

        Returns:
            pd.DataFrame: Data from NOAA
        """
        
        url = self._urljoin(base_url, "dataset=", dataset,
        "&dataTypes=", data_type, "&stations=", stations, "&startDate=",
        start_date, "&endDate=", end_date,
        "&includeAttributes=0&units=standard&format=json")

        resp = requests.get(url)

        if resp.status_code == 200:
            json_data = resp.json() 
            return pd.DataFrame(json_data)
        else:
            print("Request Failed")
    
    
    @staticmethod        
    def min_max_date(file_path:str="./data/motor_collision_data/*",
                     date_format:str="%Y-%m-%d")-> tuple[str, str]:
        """Extracts min and max date from collision data.

        Args:
            file_path (str, optional): Location of files. \
                Defaults to "./data/motor_collision_data/*".
            date_format (str, optional): Date file format. \
                Defaults to "%Y-%m-%d".

        Returns:
            tuple[str, str]: Start and end date
        """
        files = glob.glob(file_path)
        observation_dates = []
        for file in files:
            base_date = os.path.basename(file)
            date_extracted = str(base_date).replace("_","-")
            date_formatted = datetime.datetime\
                .strptime(date_extracted, date_format).date()
            observation_dates.append(date_formatted)
        min_date = min(observation_dates).strftime(date_format)
        max_date = max(observation_dates).strftime(date_format)
        return min_date, max_date


    def extract_save_noaa_data(self)->None:
        """Extracts and save NOAA data as a pickle file compressed with zip.
        """
        
        min_date, max_date = self.min_max_date()
        
        noaa_data = self.noaa_api(start_date=min_date,end_date=max_date)
        
        file_name = f"noaa_USW00094728_\
            {min_date.replace('-','_')}-{max_date.replace('-','_')}"
        noaa_data.to_pickle(f'data/weather_data/{file_name}', compression='zip')
    
    
    @staticmethod
    def convert_pickle(input_path:str, out_path:str, format:str="csv")->None:
        """extracts pickle file and save it to json or csv.

        Args:
            input_path (str): Path with file name
            out_path (str): File destination
            format (str, optional): csv | json. Defaults to "csv".
        """
        file_name = os.path.join (out_path, os.path.basename(input_path))
        data = pd.read_pickle(input_path, compression="zip")
        if format=="csv":
            data.to_csv(file_name+".csv", index=False)
        elif format =="json":
            data.to_json(file_name+".json",orient="records")
        else:
            print("Format not implemented")
    
    
    @staticmethod    
    def folder_creator(folder_paths:list[str])->None:
        """If path does not exist will create folder.

        Args:
            folder_paths (list[str]): Folders to check
        """
        for path in folder_paths:
            if not os.path.exists(path):
                os.mkdir(path)
                print(f"{path} Folder Created")
            else:
                print(f"{path} Exists")
                
    
    def initialize_project(self)->None:
        """Checks that path and folders exist for project.
        """
        
        paths = ["./data/staging_weather/",
                 "./data/staging_collision/",
                 "./data/output/",
                 "./data/output/rejected_records/",
                 "./data/log/"]
        
        self.folder_creator(paths)
        
        if not os.path.isfile('./data/log/file.log'):
            open('./data/log/file.log', 'w+').close()
            print("Created project log.")
        
        if len(os.listdir("./data/staging_collision/")) == 0:
            
            print("Extracting motor pickle files")
            
            mtr_files = glob.glob("./data/motor_collision_data/*")
            
            for item in mtr_files:
                
                self.convert_pickle(item, out_path="./data/staging_collision/")
                
        elif len(os.listdir("./data/staging_weather/")) == 0:
            
            print("Extracting weather pickle files")
            
            noa_files = glob.glob("./data/weather_data/*")
            
            for item in noa_files:
                
                self.convert_pickle(noa_files,
                            out_path="./data/staging_weather/", format="json")
        else:
            print("All folders contain files")        
        
    

class TestSparkData:
    """Class used for testing output of data pipeline.
    """
    
    @staticmethod
    def range_test(spark_df:SparkDataFrame,
                   test:list[tuple[str, int, int]])->list:
        """Test if value is in between two ranges

        Args:
            spark_df (SparkDataFrame): Data
            test (list[tuple[str, int, int]]): Test

        Returns:
            list: Test and None or Test and id of row failure
        """
        data_lst = []
        for col_name, min_val, max_val in test:
            
            tst_col_name = f"test_{col_name}_range_{min_val}_{max_val}"
            
            tst_result = spark_df.select(F.col('id'), F.col(col_name),
            F.col(col_name).between(min_val, max_val).alias(tst_col_name))\
            .filter(F.col(tst_col_name)==False).select(F.collect_list('id'))\
            .collect()[0][0]
            
            if len(tst_result)>0:
                tup_lst = list(zip([tst_col_name] * len(tst_result),tst_result))
                data_lst.append(tup_lst)
            else:
                data_lst.append((tst_col_name, None))
        return data_lst
    
    @staticmethod
    def null_test(spark_df:SparkDataFrame, test:list[str])->list:
        """Test if column has null values.

        Args:
            spark_df (SparkDataFrame): Data
            test (list[str]): Test

        Returns:
            list: Test and None or Test and id of row failure
        """
        data_lst = []
        for col_name in test:
            tst_col_name = f"test_{col_name}_null"
            tst_result = spark_df.select(F.col('id'), F.col(col_name),
                        F.col(col_name).isNull().alias(tst_col_name))\
                        .filter(F.col(tst_col_name)==True)\
                        .select(F.collect_list('id')).collect()[0][0]
        
            if len(tst_result)>0:
                tup_lst = list(zip([tst_col_name] * len(tst_result),tst_result))
                data_lst.append(tup_lst)
            else:
                data_lst.append((tst_col_name, None))
        return data_lst


    @staticmethod
    def duplicate_test(spark_df:SparkDataFrame, test:list[str])->list:
        """Test if values in column has duplicates.

        Args:
            spark_df (SparkDataFrame): Data
            test (list[str]): Test

        Returns:
            list: Test and None or Test and id of row failure
        """
        data_lst = []
        for col_name in test:
            tst_col_name = f"test_{col_name}_duplicate"
            
            tst_result = spark_df.groupBy(col_name).count().where("count > 1")\
                .drop("count").select(F.collect_list(col_name)).collect()[0][0]
            
            if len(tst_result)>0:
                dup_values = spark_df.select(F.col('id'), F.col(col_name))\
                    .filter(F.col(col_name).isin(tst_result)==True)\
                    .select(F.collect_list('id')).collect()[0][0]
                
                tup_lst =list(zip([tst_col_name] * len(dup_values), dup_values))
                data_lst.append(tup_lst)
            else:
                data_lst.append((tst_col_name, None))
        return data_lst

    @staticmethod
    def length_test(spark_df:SparkDataFrame, test:list)->list:
        """Test length of value.

        Args:
            spark_df (SparkDataFrame): Data
            test (list): Test

        Returns:
            list: Test and None or Test and id of row failure
        """
        data_lst = []
        for col_name, tgt_val in test:
            tst_col_name = f"test_{col_name}_length_{tgt_val}"
            tst_result = spark_df.select(F.col('id'), F.col(col_name),
                        F.length(F.col(col_name)).alias(tst_col_name))\
                        .filter(F.col(tst_col_name)!=tgt_val)\
                        .select(F.collect_list('id')).collect()[0][0]
            
            if len(tst_result)>0:
                tup_lst =list(zip([tst_col_name] * len(tst_result), tst_result))
                data_lst.append(tup_lst)
            else:
                data_lst.append((tst_col_name, None))
        return data_lst


    @staticmethod
    def evaluate_test(spark:object, test_data:list[list[tuple[str, any]]])-> dict:
        """Evaluates lists of test.

        Args:
            spark (object): Spark
            test_data (list[list[tuple[str, any]]]): Test result data

        Returns:
            dict: Evaluation data
        """
        result_dict = {}
        data = list(chain.from_iterable(test_data))
        
        schema = StructType([StructField("test_name", StringType(), False),
                        StructField("row_id", IntegerType(), True)])
        
        df = spark.createDataFrame(data, schema=schema)
        
        good_test_lst = df.select(F.col('test_name'), F.col('row_id').isNull()\
            .alias('row_id')).filter(F.col('row_id')==True)\
                .select(F.collect_list('test_name')).collect()[0][0]
        
        bad_test = df.select(F.col('test_name'),F.col('row_id'),
                    F.col('row_id').isNull().alias('test'))\
                    .filter(F.col('test')==False).select(F.col('test_name'),
                    F.col('row_id')).groupBy("row_id").pivot("test_name")\
                    .count()
        
        if bad_test.count()>0:
            fail_cols = [x for x in bad_test.columns if x not in ['row_id']]
            result_dict['fail_data'] = bad_test
            result_dict['fail_cols'] = fail_cols
        
        result_dict['pass_cols'] = good_test_lst
               
        return result_dict
    
    
    @staticmethod
    def print_test_results(data_source_name:str, data_dict:dict)->None:
        """Prints test results.

        Args:
            data_source_name (str): Name of data
            data_dict (dict): Test evaluation results
        """
        print("*"*25, data_source_name, "*"*25 )
        for key, value in data_dict.items():
            if key == 'pass_cols':
                print('*'*25,'Successful Test','*'*25)
                print(*value, sep='\n')
                print('*'*50)
            if key == 'fail_cols':
                    print('*'*25,'Failed Test','*'*25)
                    print(*value, sep='\n')
                    print('*'*50)



class ETL:
    """Create pipeline for NOAA and Motor collision data.
    """
    
    def __init__(self, spark:object,
                 noaa_data_path:str="./data/staging_weather",
                 motor_data_path:str="./data/staging_collision",
                 output_path:str="./data/output",
                 noaa_cols:list=['AWND', 'DATE', 'PRCP', 'SNOW', 'STATION',
                                 'TMIN', 'TMAX', 'WT01'],
                 motor_cols:list=['COLLISION_ID','CRASH DATE','CRASH TIME',
                                  'BOROUGH','ZIP CODE','LATITUDE','LONGITUDE',
                                  'ON STREET NAME','CROSS STREET NAME',
                                  'OFF STREET NAME','NUMBER OF PERSONS INJURED',
                                  'NUMBER OF PERSONS KILLED'],
                 exclude_failed_records=True):
        """Entry point for ETL Class

        Args:
            spark (object): Spark
            noaa_data_path (str, optional): Extracted weather data path  in\
                json format. Defaults to "./data/staging_weather".
            motor_data_path (str, optional): Motor collision data path in \
                csv format. Defaults to "./data/staging_collision".
            output_path (str, optional): Results destination. Defaults to \
                "./data/output".
            noaa_cols (list, optional): Columns of interest. Defaults to \
                ['AWND', 'DATE', 'PRCP', 'SNOW', 'STATION', 'TMIN', 'TMAX'\
                    , 'WT01'].
            motor_cols (list, optional): Columns of interest. Defaults\
                to ['COLLISION_ID','CRASH DATE','CRASH TIME', 'BOROUGH',\
                    'ZIP CODE','LATITUDE','LONGITUDE', 'ON STREET NAME',\
                        'CROSS STREET NAME', 'OFF STREET NAME',\
                            'NUMBER OF PERSONS INJURED',\
                                'NUMBER OF PERSONS KILLED'].
            exclude_failed_records (bool, optional): If records fail testing\
                exclude.Defaults to True.
        """
        self.spark = spark
        self.noaa_data_path = noaa_data_path
        self.motor_data_path = motor_data_path
        self.output_path = output_path
        self.noaa_cols = noaa_cols
        self.motor_cols = motor_cols
        self.exc_failed_recs = exclude_failed_records
        self.measures = {}

    @staticmethod
    def clean_address(spark_df:SparkDataFrame,
                      columns:list[str])->SparkDataFrame:
        """Clean address data.

        Args:
            spark_df (SparkDataFrame): Data
            columns (list[str]): Target columns

        Returns:
            SparkDataFrame: Data
        """
        for column in columns:
            try:
                spark_df = spark_df.withColumn(column, F.trim(F.col(column)))\
                    .withColumn(column, F.encode(F.col(column),"US-ASCII") )\
                    .withColumn(column, F.decode(F.col(column),"US-ASCII") )\
                    .withColumn(column, F.upper(F.regexp_replace(F.col(column)\
                    , "[^a-zA-Z\\s0-9]", "")))
            except Exception as e:
                    logging.error("ETL clean_address Exception ", exc_info=True)
                    continue
        return spark_df
    
    @staticmethod
    def change_column_type(spark_df:SparkDataFrame, 
                           changes:list[tuple[str, str]])->SparkDataFrame:
        """Change column type

        Args:
            spark_df (SparkDataFrame): Data
            changes (list[tuple[str, str]]): Changes

        Returns:
            SparkDataFrame: Data
        """
        for column, column_type in changes:
            try:
                spark_df = spark_df.withColumn(column,F.col(column)\
                    .cast(column_type))
            except Exception as e:
                logging.error("ETL change_column_type Exception", exc_info=True)
                continue
                
        return spark_df
 
    @staticmethod
    def fill_nulls(spark_df:SparkDataFrame, 
           changes:list[tuple[str, str, Union[str,int,float]]])->SparkDataFrame:
        """Fill null with desired value.

        Args:
            spark_df (SparkDataFrame): Data
            changes (list[tuple[str, str, Union[str,int,float]]]): Changes

        Returns:
            SparkDataFrame: Data
        """
        for column, column_type, fill_val in changes:
            try:
                spark_df = spark_df\
                    .withColumn(column,F.coalesce(F.col(column)\
                        .cast(column_type), F.lit(fill_val)))
            except Exception as e:
                logging.error("ETL fill_nulls Exception ", exc_info=True)
                continue
                
        return spark_df
    
    @staticmethod
    def extract_date(spark_df:SparkDataFrame,
                     columns:list[str] )->SparkDataFrame:
        """Extract date information from date formatted column.

        Args:
            spark_df (SparkDataFrame): Data
            columns (list[str]): Column

        Returns:
            SparkDataFrame: Data
        """
        for column in columns:
            
            try:
                spark_df = spark_df\
                    .withColumn(f"{column}_year", F.year(F.col(column)))\
                    .withColumn(f"{column}_month", F.month(F.col(column)))\
                    .withColumn(f"{column}_dayofmonth",
                                F.dayofmonth(F.col(column)))
            except Exception as e:
                logging.error("ETL extract_date Exception ", exc_info=True)
                continue
                
        return spark_df
      
    def clean_motor_data(self)->SparkDataFrame:
        """Cleans motor collision data.

        Returns:
            SparkDataFrame: Data
        """
        
        file_len = len(glob.glob(os.path.join(self.motor_data_path,"*.csv")))
        logging.info(f"ETL:clean_motor_data will process {file_len} files")
        if file_len >0:
            try:
                
                self.measures["mtr_file_bytes"]\
                    = sum(d.stat().st_size for d in \
                        os.scandir(self.motor_data_path) if d.is_file())
                    
                logging.info("ETL:clean_motor_data loading data")
                data = self.spark.read.option("delimiter", ",")\
                    .option("header", True).option("inferSchema", "true")\
                    .option("nanValue", "null",).csv(self.motor_data_path)\
                    .select(self.motor_cols).cache()
                    
                self.measures["mtr_intial_rows"] = data.count()
                    
                street_lst = ["ON STREET NAME", "CROSS STREET NAME",
                              "OFF STREET NAME"]
                logging.info(f"ETL:clean_motor_data cleaning {street_lst}")
                data = self.clean_address(data, street_lst)
                
                
                fix_null_lst = [('NUMBER OF PERSONS INJURED','int', 0),
                                ('NUMBER OF PERSONS KILLED','int', 0)]
                logging.info(f"ETL:clean_motor_data fix nulls {fix_null_lst}")
                data = self.fill_nulls(data, fix_null_lst)

                logging.info("ETL:clean_motor_data cleaning LATITUDE")
                data = data.withColumn("LATITUDE",
                    F.when(((F.col("LATITUDE") <-90) |(F.col("LATITUDE")>90)),
                    F.lit(None)).otherwise(F.col("LATITUDE")))
                
                logging.info("ETL:clean_motor_data cleaning LONGITUDE")
                data = data.withColumn("LONGITUDE",
                    F.when(((F.col("LONGITUDE")<-180)|(F.col("LONGITUDE")>180)),
                    F.lit(None)).otherwise(F.col("LONGITUDE")))                
            
                logging.info("ETL:clean_motor_data Extracting date information")
                data = self.extract_date(data, ["CRASH DATE"])
               
                logging.info("ETL:clean_motor_data Extracting time information")
                data = data.withColumn("hour", F.hour(F.col("CRASH TIME")))\
                    .withColumn("minute", F.minute(F.col("CRASH TIME")))
            
                logging.info("ETL:clean_motor_data Creating col incident_level")
                data = data.withColumn('incident_level',
                    F.when(data['NUMBER OF PERSONS INJURED'] > 0, 'Medium')\
                    .when(data['NUMBER OF PERSONS KILLED'] > 0, 'High')\
                    .otherwise('Low'))
                
                logging.info("ETL:clean_motor_data dropping date & time cols")
                data = data.drop('CRASH DATE', 'CRASH TIME')
                
                type_lst = [("ZIP CODE", 'int'),("COLLISION_ID", 'int')]
                logging.info(f"ETL:clean_motor_data casting {type_lst}")
                data = self.change_column_type(data, type_lst)

                logging.info("ETL:clean_motor_data casting ZIP & ID as int")
                data = data.withColumn('ID', F.monotonically_increasing_id())
                
                logging.info("ETL:clean_motor_data formatting columns")
                data = data.toDF(*(re.sub(r'[\.\s]+', '_', c)\
                    .lower() for c in data.columns))
  
                return data
            
            except Exception as e:
                logging.error("*"*25+"Exception"+"*"*25) 
                logging.error("Exception occurred", exc_info=True)       
        else:
            logging.critical("ETL:clean_motor_data No file to process")
      
    
    def clean_noaa_data(self)->SparkDataFrame:
        """Cleans NOAA data

        Returns:
            SparkDataFrame: Data
        """
        
        file_len = len(glob.glob(os.path.join(self.noaa_data_path,"*.json")))
        logging.info(f"ETL:clean_noaa_data will process {file_len} files")
        if file_len >0:
            try:

                self.measures["noa_file_bytes"]\
                    = sum(d.stat().st_size for d in \
                        os.scandir(self.noaa_data_path) if d.is_file())
                    
                logging.info("ETL:clean_noaa_data loading data")
                data = self.spark.read.option("multiline", "true")\
                    .option("inferSchema", "true").option("nanValue", "null",)\
                    .json(self.noaa_data_path).select(self.noaa_cols).cache()
                    
                self.measures["noa_intial_rows"] = data.count()
                
                type_lst = [("AWND", 'float'), ("PRCP", 'float'),
                            ("PRCP", 'float'), ("SNOW", 'float'),
                            ("TMIN", 'int'), ("TMAX", 'int')]
                logging.info(f"ETL:clean_noaa_data formatting {type_lst}")
                data = self.change_column_type(data, type_lst)
                
                logging.info("ETL:clean_noaa_data formatting DATE & WT01")
                data = data.withColumn("DATE",F.to_date("DATE", 'yyyy-MM-dd'))\
                    .withColumn("WT01", F.when(F.col("WT01")==1, True)\
                        .otherwise(False))
                
                fill_null_lst = [("AWND", 'float', 0.0)]
                logging.info(f"ETL:clean_noaa_data nulls {fill_null_lst}")
                data = self.fill_nulls(data, fill_null_lst)   
                
                logging.info("ETL:clean_noaa_data extracting dates")                
                data = self.extract_date(data, ["DATE"])
                
                logging.info("ETL:clean_noaa_data creating AVGTEMP") 
                data = data.withColumn("AVGTEMP",
                                    (F.col("TMIN") + F.col("TMAX"))/ F.lit(2) )
                
                logging.info("ETL:clean_noaa_data creating ID") 
                data = data.withColumn('ID', F.monotonically_increasing_id())
                
                logging.info("ETL:clean_noaa_data dropping DATE column") 
                data = data.drop("DATE")
                
                logging.info("ETL:clean_noaa_data formatting columns")
                data = data.toDF(*(re.sub(r'[\.\s]+', '_', c)\
                    .lower() for c in data.columns))
                
                return data
                
            except Exception as e:
                logging.error("*"*25+"Exception"+"*"*25) 
                logging.error("Exception occurred", exc_info=True)    
        else:
            logging.critical("ETL:clean_noaa_data No file to process")


    def main(self)->None:
        """Executes steps to run pipeline & extract metrics.
        """
        self.measures['overall_start_time'] = time.time()
        
        logging.info("ETL:main running clean_motor_data func")
        mtr_data = self.clean_motor_data()
        self.measures['mtr_end_time_row_proc_sec'] = time.time()
        
        logging.info("ETL:main running clean_noaa_data func")
        self.measures['noa_start_time_row_proc_sec'] = time.time()
        noa_data = self.clean_noaa_data()
        self.measures['noa_end_time_row_proc_sec'] = time.time()
        
        self.measures['mtr_process_rows'] = mtr_data.count()
        
        self.measures['noa_process_rows'] = noa_data.count()
        
        self.measures['overall_end_time_row_proc_sec'] = time.time()
        
        #motor
        logging.info("ETL:main creating testing scenarios for motor collision")
        mtr_null_tst_lst = ['collision_id','crash_date_year',
                            'number_of_persons_injured',
                            'number_of_persons_killed','incident_level']
        mtr_dup_tst_lst = ['collision_id']
        mtr_len_tst_lst = [('zip_code',5)] 
        mtr_range_tst_lst = [('latitude',-90,90),
                            ('longitude',-180,180),
                            ('number_of_persons_injured',0,50),
                            ('number_of_persons_killed',0,50),
                            ('crash_date_month',1,12),
                            ('crash_date_dayofmonth',1,31),
                            ('hour',0,23),
                            ('minute',0,60)]
        
        logging.info("ETL:main initiating TestSparkData Class")
        testing = TestSparkData()
        
        logging.info("ETL:main running motor tests")
        mtr_null_rslt = testing.null_test(mtr_data, mtr_null_tst_lst)
        mtr_dup_rslt = testing.duplicate_test(mtr_data, mtr_dup_tst_lst)
        mtr_len_rslt = testing.length_test(mtr_data, mtr_len_tst_lst)
        mtr_rng_rslt = testing.range_test(mtr_data, mtr_range_tst_lst)
        all_mtr_test = [mtr_null_rslt, mtr_dup_rslt, mtr_len_rslt, mtr_rng_rslt]
        
        logging.info("ETL:main evaluating motor tests")
        mtr_test_results = testing.evaluate_test(self.spark, all_mtr_test)
        
        testing.print_test_results("Motor Collision Data",mtr_test_results)
        
        #noaa
        logging.info("ETL:main creating testing scenarios for NOAA Data")
        noa_null_tst_lst = ['station', 'wt01']
        noa_range_tst_lst = [('awnd',0, 25),
                            ('prcp',0, 15),
                            ('snow',0, 25),
                            ('tmin',0, 120),
                            ('tmax',0, 120),
                            ('date_month',1,12),
                            ('date_dayofmonth',1,31),
                            ('avgtemp',0, 120)]
        
        logging.info("ETL:main running noaa tests")
        noa_null_rslt = testing.null_test(noa_data, noa_null_tst_lst)
        noa_rng_rslt = testing.range_test(noa_data, noa_range_tst_lst)
        all_noa_test = [noa_null_rslt, noa_rng_rslt]
        
        logging.info("ETL:main evaluating noa tests")
        noa_test_results = testing.evaluate_test(self.spark, all_noa_test)
        
        testing.print_test_results("NOAA Weather Data", noa_test_results)
        
        self.measures['overall_row_per_sec_start'] = time.time()
        
        if self.exc_failed_recs:
            
            if (mtr_test_results.keys() >= {"pass_cols", "fail_cols"}):
                
                mtr_pass_records = mtr_data.join(mtr_test_results['fail_data'],
                                F.col('id')==F.col('row_id'), how='left_anti')
                
                mtr_fail_records = mtr_data.join(mtr_test_results['fail_data'],
                                    F.col('id')==F.col('row_id'), how='inner')
                
                logging.info("main: Saving passing records motor")
                mtr_pass_records.write.parquet(path=os.path\
                    .join(self.output_path, "motor.parquet"), mode="overwrite")
                
                self.measures["mtr_end_rows"] = mtr_pass_records.count()
                
                logging.warn(f"main: failed records \
                    {mtr_test_results['fail_cols']}")
                mtr_fail_records.write.csv(path = os\
                    .path.join(self.output_path,"rejected_records",
                               "motor_rejected"), header=True, mode="overwrite")
                
            else:
                logging.info("main: Saving records no issues")
                self.measures["mtr_end_rows"] = mtr_data.count()
                mtr_data.write.parquet(path=os.path\
                    .join(self.output_path, "motor.parquet"), mode="overwrite")
                
            if (noa_test_results.keys() >= {"pass_cols", "fail_cols"}):
                
                noa_pass_records = noa_data\
                    .join(noa_test_results['fail_data'],
                          F.col('id')==F.col('row_id'), how='left_anti')
                
                noa_fail_records = noa_data\
                    .join(noa_test_results['fail_data']
                          , F.col('id')==F.col('row_id'), how='inner')
                
                logging.info("main: Saving passing records noaa")
                noa_pass_records.write.parquet(path=os.path\
                    .join(self.output_path, "noaa.parquet"), mode="overwrite")
                
                self.measures["noa_end_rows"] = noa_pass_records.count()
                
                logging.warn(f"main: failed records noaa \
                    {noa_test_results['fail_cols']}")
                noa_fail_records.write.csv(path = os.path\
                    .join(self.output_path,"rejected_records", "noaa_rejected"),
                    header=True, mode="overwrite")
                
            else:
                logging.info("main: Saving records no issues noa")
                self.measures["noa_end_rows"] = noa_data.count()
                noa_data.write.parquet(path=os.path\
                    .join(self.output_path, "noaa.parquet"), mode="overwrite")
                
        else:
            logging.info("main: Saving motor & noa exc_failed_recs is False")
            
            self.measures["mtr_end_rows"] = mtr_data.count()
            
            mtr_data.write.parquet(path=os.path\
                .join(self.output_path, "motor.parquet"), mode="overwrite")
            
            self.measures["noa_end_rows"] = noa_data.count()
            noa_data.write.parquet(path=os.path\
                .join(self.output_path, "noaa.parquet"), mode="overwrite")
        self.measures['overall_row_per_sec_end'] = time.time()
 
        self.measures['overall_end_time'] = time.time()
        
        logging.info("main: Calculating measures")
        
        self.measures['stat_overall_total_time_sec']\
        = self.measures['overall_end_time']\
        - self.measures['overall_start_time']
        
        self.measures['stat_overall_rows_process_per_sec'] \
            = (self.measures['mtr_process_rows']\
            + self.measures['noa_process_rows'])\
            / (self.measures['overall_end_time_row_proc_sec']\
            - self.measures['overall_start_time'])
        
        self.measures['stat_mtr_rows_process_per_sec']\
            = (self.measures['mtr_process_rows']\
            /(self.measures['mtr_end_time_row_proc_sec']\
            - self.measures['overall_start_time']))
        
        self.measures['stat_noa_rows_process_per_sec']\
            =(self.measures['noa_process_rows']\
            /(self.measures['noa_end_time_row_proc_sec']\
            - self.measures['noa_start_time_row_proc_sec']))
            
        self.measures['stat_overall_throughput_bytes_per_sec']\
            =  (self.measures['mtr_file_bytes']\
            + self.measures['noa_file_bytes'] )\
            / (self.measures['overall_end_time']\
            - self.measures['overall_start_time'])
            
        util = Utilities()
        
        self.measures['stat_overall_throughput_MB_per_sec']\
            =  (util.byte_size(self.measures['mtr_file_bytes'])\
            + util.byte_size(self.measures['noa_file_bytes']) )\
            / (self.measures['overall_end_time']\
            - self.measures['overall_start_time'])
        
        self.measures['stat_overall_rows_written_per_sec']\
            =  (self.measures['mtr_process_rows']\
            + self.measures['mtr_process_rows'] )\
            /(self.measures['overall_row_per_sec_end']\
            - self.measures['overall_row_per_sec_start'])
            
        self.measures['stat_motor_source_count']\
            = f'source {self.measures["mtr_intial_rows"]}\
                destination {self.measures["mtr_end_rows"]}\
                values equal\
            {self.measures["mtr_intial_rows"] == self.measures["mtr_end_rows"]}'
        
        self.measures['stat_noaa_source_count']\
            = f'source {self.measures["noa_intial_rows"]}\
                destination {self.measures["noa_end_rows"]}\
                values equal\
            {self.measures["noa_intial_rows"] == self.measures["noa_end_rows"]}'
        
        logging.info(f"main: info {self.measures}")
        
        print('*'*25, "Pipeline Measures" ,'*'*25)
        
        for key, value in self.measures.items():
            if key.startswith("stat_"):
                print('*'*25, key[5:] ,'*'*25)
                print(value )
