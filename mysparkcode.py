from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data1 = "C:\Bigdata\drivers\world_bank.json"
df = spark.read.format('json').option('mode','dropmalformed').load(data1)
#df.printSchema()

#remove array using explode function()

res = df.withColumn('theme1name',col('theme1.Name')).withColumn('theme1percent',col('theme1.Percent'))\
    .drop('theme1').withColumn('theme_namecode',explode(col('theme_namecode')))\
    .withColumn('majorsector_percent',explode(col('majorsector_percent')))\
    .withColumn('mjsector_namecode',explode(col('mjsector_namecode')))\
    .withColumn('mjtheme',explode(col('mjtheme')))\
    .withColumn('mjtheme_namecode',explode(col('mjtheme_namecode')))\
    .withColumn('projectdocs',explode(col('projectdocs')))\
    .withColumn('sector',explode(col('sector')))\
    .withColumn('sector_namecode',explode(col('sector_namecode')))

#res.printSchema()

CorrHead = res.withColumn('Id',col('_id.$oid')).drop('_id').withColumn('majorsector_percent_Name',col('majorsector_percent.Name'))\
            .withColumn('majorsector_percent_Percent',col('majorsector_percent.Percent')).drop('majorsector_percent')\
            .withColumn('mjsector_namecode_Code',col('mjsector_namecode.code'))\
            .withColumn('mjsector_namecode_Name',col('mjsector_namecode.name')).drop('mjsector_namecode')\
            .withColumn('mjtheme_namecode_Code',col('mjtheme_namecode.code'))\
            .withColumn('mjtheme_namecode_Name',col('mjtheme_namecode.name')).drop('mjtheme_namecode')\
            .withColumn('project_abstract_Cdata',col('project_abstract.cdata')).drop('project_abstract')\
            .withColumn('projectdocs_DocDate',col('projectdocs.DocDate'))\
            .withColumn('projectdocs_DocType',col('projectdocs.DocType'))\
            .withColumn('projectdocs_DocTypeDesc',col('projectdocs.DocTypeDesc'))\
            .withColumn('projectdocs_DocURL',col('projectdocs.DocURL'))\
            .withColumn('projectdocs_EntityID',col('projectdocs.EntityID')).drop('projectdocs')\
            .withColumn('sector_Name',col('sector.Name')).drop('sector')\
            .withColumn('sector1_Name',col('sector1.Name'))\
            .withColumn('sector1_Percent',col('sector1.Percent')).drop('sector1')\
            .withColumn('sector2_Name',col('sector2.Name'))\
            .withColumn('sector2_Percent',col('sector2.Percent')).drop('sector2')\
            .withColumn('sector3_Name',col('sector3.Name'))\
            .withColumn('sector3_Percent',col('sector3.Percent')).drop('sector3')\
            .withColumn('sector4_Name',col('sector4.Name'))\
            .withColumn('sector4_Percent',col('sector4.Percent')).drop('sector4')\
            .withColumn('sector_namecode_Code',col('sector_namecode.code'))\
            .withColumn('sector_namecode_Name',col('sector_namecode.name')).drop('sector_namecode')\
            .withColumn('theme_namecode_Code',col('theme_namecode.code'))\
            .withColumn('theme_namecode_Name',col('theme_namecode.name')).drop('theme_namecode')




