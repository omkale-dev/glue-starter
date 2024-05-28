import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import importlib
from pyspark.sql.functions import col, coalesce, lit
import helper_functions
importlib.reload(helper_functions)

LOCAL= True
job_name = "the_glue_job_one"

source_path = "s3://<bucket>/department_data.csv"
mapping_path = f"./jupyter_workspace/{job_name}/the_resources/mapping.csv"

def glue_init():
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    if not LOCAL:
        global source_path, mapping_path
        args = getResolvedOptions(sys.argv,["source_path"])
        source_path=args["source_path"]
        print(source_path)
        mapping_path = "./mapping.csv"
   
    return spark, glueContext, job


def extract(glueContext, source_file):
    source_df = helper_functions.get_source_df(glueContext, source_file)
    return source_df


# Main
if __name__ == "__main__":
    spark, glueContext, job = glue_init()
    mapping = helper_functions.get_csv_mapping(mapping_path)
    print("Mapping:",mapping)
    source_df = extract(glueContext, source_path)
    source_df.show()


