import sys
from random import random
from operator import add
from pyspark.sql import SQLContext, Row
from pyspark import SparkContext
from pyspark.sql import DataFrameWriter

if __name__ == "__main__":
	sc = SparkContext(appName="WeatherDataAnalysis")
	sqlContext = SQLContext(sc)
	for num in range(2000,2018):
		file = "hdfs:/user/tatavag/PIIweather/"+str(num)+".csv"
		Parselines = sc.textFile(file)
		SplitLine = Parselines.map(lambda l: l.split(","))
		val = SplitLine.map(lambda p: Row(ID=p[0], DATE=p[1], ELEMENT=p[2], VALUE=int(p[3]), MF=p[4], QF=p[5], SF=p[6]))
		weatherdata = sqlContext.createDataFrame(val)
		weatherdata.registerTempTable("weather")
		analysisone = sqlContext.sql("SELECT ELEMENT, avg(VALUE) VALUE from weather where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ELEMENT")
		analysisone.show()
		analysistwo = sqlContext.sql("SELECT ELEMENT, IF(ELEMENT='TMAX',MAX(VALUE), MIN(VALUE)) VALUE from weather where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ELEMENT")
		analysistwo.show()
		analysisthreeA = sqlContext.sql("SELECT ID, MIN(VALUE) as Val from weather where ELEMENT='TMIN' AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' GROUP BY ID ORDER BY Val LIMIT 5")
		analysisthreeA.show()
		analysisthreeB = sqlContext.sql("SELECT ID, MAX(VALUE) as Val from weather where ELEMENT='TMAX' AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' GROUP BY ID ORDER BY Val DESC LIMIT 5")
		analysisthreeB.show()
		analysisfourA = sqlContext.sql("SELECT ID, DATE, VALUE from weather where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ID, DATE, VALUE ORDER BY VALUE LIMIT 1")
		analysisfourA.show()
		analysisfourB = sqlContext.sql("SELECT ID, DATE, VALUE from weather where ELEMENT in ('TMIN','TMAX') AND VALUE <> 9999 AND QF IN ('','Z','W') AND SF <> '' group by ID, DATE, VALUE ORDER BY VALUE DESC LIMIT 1")
		analysisfourB.show()
		analysisone.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/chintasd/testing1/"+str(num)+"result.csv")
		analysistwo.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/chintasd/testing2/"+str(num)+"result.csv")
		analysisthreeA.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/chintasd/testing3_1/"+str(num)+"result.csv")
		analysisthreeB.rdd.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/chintasd/testing3_2/"+str(num)+"result.csv")
		analysisfourA.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/chintasd/testing4_1/"+str(num)+"result.csv")
		analysisfourB.map(lambda x: ",".join(map(str,x))).coalesce(1).saveAsTextFile("hdfs:/user/chintasd/testing4_2/"+str(num)+"result.csv")
	sc.stop()
