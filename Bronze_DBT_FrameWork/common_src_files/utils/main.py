from pyspark.sql import SparkSession, DataFrame


def validation(spark):
  print("validation goes here")

def get_spark() -> SparkSession:
  try:
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.getOrCreate()
  except ImportError:
    return SparkSession.builder.getOrCreate()
  

def main():
  validation(get_spark())

if __name__ == '__main__':
  main()