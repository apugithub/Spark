# Reading data from nested JSON
'''Input =
{
  "Market Data": {
    "Data Flow": {
      "Dataflow1": "ST",
      "Dataflow2": "YT"
    },
    "Price Publication Information": [
      {
        "ID": "MAX98I",
        "ISIN": "NBNB",
        "FIGI": "7887",
        "layer": {
          "one": "YUYHG",
          "two": "HGH"
        }
      },
      {
        "ID": "56TY",
        "ISIN": "999",
        "FIGI": "111",
        "layer": {
          "one": "YUYHG",
          "two": "HGH"
        }
      }
    ]
  }
}

Output =  in CSV only need 'Price Publication Information' fields along with "layer" fields '''

from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id

df = spark.read.json('/FileStore/tables/test2json.txt', multiLine=True, mode = "PERMISSIVE")
df1 = df.select(F.explode('Market Data.Price Publication Information').alias('tmp')).select('tmp.*').drop('layer')
df2 = df.select(F.explode('Market Data.Price Publication Information.layer').alias('tmp1')).select('tmp1.*')

df1 = df1.withColumn("id", monotonically_increasing_id())
df2 = df2.withColumn("id", monotonically_increasing_id())
df3 = df1.join(df2, "id", "outer").drop("id")

df1.coalesce(1).write.option("inferSchema","true").csv("/FileStore/tables/test1json_op", header = 'true', mode='append')

df3.show()
df1.printSchema()

