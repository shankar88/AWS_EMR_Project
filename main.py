from pyspark.sql import SparkSession


s3_input = "s3://emr-athena-gowri/source-folder/market.csv"
s3_output = "s3://emr-athena-gowri/output/"


def main():
    spark = SparkSession.builder.appName("emrProject").getOrCreate()
    df = spark.read.option("header",True).csv(s3_input)
    print(f'The total number of records in the source data set is {df.count()}')
    filtered_df = df.filter((df.Product_line == "Electronic accessories"))
    print(f'The total number of records in the filtered data set is {filtered_df.count()}')
    filtered_df.show(10)
    filtered_df.printSchema()
    filtered_df.write.mode('overwrite').parquet(s3_output)
    print('The filtered output is uploaded successfully')

if __name__ == '__main__':
    main()
