from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import *
#Chat project with batch processing
sp= SparkSession.builder.enableHiveSupport().getOrCreate()
lines=sp.sparkContext.textFile("file:/home/hduser/chatdata.txt")
p=lines.map(lambda l:l.split("~"))
chat = p.map(lambda c: (c[0], c[1], c[2].strip()))
schema = StructType([StructField("id", StringType(), True), StructField("chat", StringType(), True),StructField("type",StringType(),True)])
data=sp.createDataFrame(chat,schema)
rm=data.drop('type')
rm.createOrReplaceTempView("chat")
result=sp.sql("select * from chat")
chart_split=result.select('id',f.split('chat',' ').alias('chat'))
chart_split.show()
explode=chart_split.select(chart_split.id,explode(chart_split.chat))
explode.createOrReplaceTempView("chattempview")
chattempview=sp.sql("select * from chattempview").show()
#Stop word file
df1 = sp.read.load("file:/home/hduser/stopwords.csv", format="csv", inferSchema="True", header="True")
#df1.show(10)
df1.createOrReplaceTempView("stoptempview")
stoptempview=sp.sql("select * from stoptempview").show()
#Left join on stoptempview and chat tempview
#sql=sp.sql("select c.id,c.col from chattempview c left join stoptempview s on (c.col=s.stopword) where s.stopword is not null ")
sql=sp.sql("select * from chattempview where col not in (select * from stoptempview)")
sql.show()
final=sql.select('id','col')
final.createOrReplaceTempView("final_tbl")
sp.sql("select * from final_tbl").show()
sp.sql("create table default.final_tbl as select * from final_tbl")
sp.sql("select col,count(*) from final_tbl group by col").show()







