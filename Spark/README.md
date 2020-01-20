# Spark Quick Commands

### Command to run  a program in local Windows env
#### Start a spark-shell with configuration 
`spark-shell --conf spark.sql.shuffle.partitions=1`

#### To check memory usage
`free -m`

##### To free spark sql cache
`sqlContext.clearCache()`

##### To reboot the system if Hive not working
sudo init 6

##### If Hive size in hive-site.xml is 256 , you can increase it using hiveconf paramater like below
`hive -hiveconf hive.tez.container.size=1024`

### Command to run in local
```
spark-submit2 
--class "wordcount.WordCount" 
--master local[*] 
C:\PROJECTS\SparkWorkspace\word-count\target\scala-2.11\word-count_2.11-1.0.jar 
local file:///C:/PROJECTS/SparkWorkspace/data/input/politicsnews file:///C:/PROJECTS/SparkWorkspace/data/output/word-count
```

### Command to run in standalone mode
```
spark-submit2 
--class "WordCount" 
--master spark://localhost:7077 
C:\PROJECTS\SparkWorkspace\simple-spark\target\scala-2.11\simple-spark_2.11-1.0.jar 
local file:///C:/PROJECTS/SparkWorkspace/data/input/politicsnews file:///C:/PROJECTS/SparkWorkspace/data/output/word-count
```

### How to read a file
`val numRDD = sc.textFile("file:///C:/PROJECTS/SparkWorkspace/data/input/numbers.txt")`

```
spark-submit2 
--class "wordcount.WordCount" 
--master local[*] 
C:\PROJECTS\SparkWorkspace\word-count\target\scala-2.11\word-count_2.11-1.0.jar
```

### How to submit a Spark Job in Local mode in Sandbox environment

`spark-submit --class "SimpleApp" \
simple-app_2.11-1.0.jar local  /user/sparkuser/myspark/data/output/simple-app`

### How to submit a Spark Job in Yarn mode(with memory parameters) in Sandbox environment
```
spark-submit --class "SimpleApp" \
--master yarn \
--executor-memory 512m \
--total-executor-cores 1 \
simple-app_2.11-1.0.jar yarn-client  /user/sparkuser/myspark/data/output/simple-app
```

```spark-submit \
 --class myapps.Orders \
 simple-app_2.11-1.0.jar \
 local[2] /user/sparkuser/myspark/data/input/Joyce.txt /user/sparkuser/myspark/data/output/joyce-file
 ```

 ```
 spark-submit \
 --master yarn-client \
 --executor-memory 512m \
 --total-executor-cores 1 \
 --class myapps.Orders \
 simple-app_2.11-1.0.jar \
 yarn-client /user/sparkuser/myspark/data/input /user/sparkuser/myspark/data/output/orders
 ```
 
```spark-submit \
 --master yarn-cluster \
 --deploy-mode cluster \
 --driver-memory 512m \
 --executor-memory 512m \
 --total-executor-cores 1 \ 
 --class myapps.MovieLens \
   simple-app_2.11-1.0.jar \
   yarn-cluster /user/sparkuser/myspark/data/input /user/sparkuser/myspark/data/output/movie-lens
   ```
  
Path for  Orders : `local /user/sparkuser/myspark/data/input /user/sparkuser/myspark/data/output/orders`

Path for  Joyce : `local /user/sparkuser/myspark/data/input/Joyce.txt /user/sparkuser/myspark/data/output/joyce-file`

Path for  Movie Lens : `local /user/sparkuser/myspark/data/input /user/sparkuser/myspark/data/output/movie-lens`

```
$ ./bin/spark-submit \
--master spark://hostname:7077 \
--deploy-mode cluster \
--class com.databricks.examples.SparkExample \
--name "Example Program" \
--jars dep1.jar,dep2.jar,dep3.jar \
--total-executor-cores 300 \
--executor-memory 10g \
myApp.jar "options" "to your application" "go here"
```
### Submitting a Python application in YARN client mode
`export HADOP_CONF_DIR=/opt/hadoop/conf`
```
$ ./bin/spark-submit \
--master yarn \
--py-files somelib-1.2.egg,otherlib-4.4.zip,other-file.py \
--deploy-mode client \
--name "Example Program" \
--queue exampleQueue \
--num-executors 40 \
--executor-memory 10g \
my_script.py "options" "to your application" "go here"
```

###### To check cpu information in linux system
`lscpu`

###### To check no. of cores in linux system
`nproc`

###### To check in Windows , Go to command prompt, open run prompt and enter "msinfo32.exe"

## Best Practices to optimize Spark scripts
1. Prefer Spark SQL over RDDs
2. Create DFs and register them in Temp tables and use these temp tables to perform joins and select. It improves performance
3. Use coalesce only when partitions are to be reduced. Default number of partitions in DF is 200.



### Pg no 52 of Learning Spark Exercise to calculate average.  For calculating avergae we need to keep two computations - one is total of all the values and next is total count of values.

#### Load the RDD from external file
`val input = sc.textFile("file:///C:/Projects/SparkWorkspace/data/input/pairs.txt")`

#### Map the values and cconvert numeric value to Int instead of String
`val mapRDDs = input.map(x => (x.split(",")(0),x.split(",")(1).toInt))`

res77: Array[(String, Int)] = Array((panda,0), (pink,3), (pirate,3), (panda,1), (pink,4))

#### Now apply mapValues so that 1 is kept for each value in order to get count of values
`mapRDDs.mapValues(x => (x,1))`
or
`input.map(_.split(",")).map(v => (v(0),v(1).toInt))`

res79: Array[(String, (Int, Int))] = Array((panda,(0,1)), (pink,(3,1)), (pirate,(3,1)), (panda,(1,1)), (pink,(4,1)))

#### Now apply reduceByKey to get aggregate for each key
`mapRDDs.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))`
res80: Array[(String, (Int, Int))] = Array((panda,(1,2)), (pink,(7,2)), (pirate,(3,1)))

###### AggregateByKey is more efficient that using above method  (To try own)

## SPARK SQL
### To load data as DF using case class
1. Load a normal text file
`val input  = sc.textFile("file:///home/technocrafty/shalini/spark/input/pairs.txt")`

input: org.apache.spark.rdd.RDD[String] = file:///C:/PROJECTS/SparkWorkspace/data/input/pairs.txt MapPartitionsRDD[1] at textFile at <console>:24

2. Create a case class
`case class Pair (Name:String,Count:Int)`

defined class Pair

3.  Split data by "," and convert into Array 
`val pairRDD = input.map(x=>x.split(","))`
 pairRDD: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[2] at map at <console>:26
 
4. `val pairDF = pairRDD.map(pair => Pair(pair(0),pair(1).toInt)).toDF`
pairDF: org.apache.spark.sql.DataFrame = [Name: string, Count: int]

5. To print the all the contents of DF use
`pairDF.collect()`
res8: Array[org.apache.spark.sql.Row] = Array([panda,0], [pink,3], [pirate,3], [panda,1], [pink,4])

6. OR Register as a temp table to get output using SQL query
 `pairDF.registerTempTable("pairs")`

Now Use sqlContext to query output
`sqlContext.sql("select * from pairs").collect()`
or 
`sqlContext.sql("select * from pairs").foreach(println)`
res11: Array[org.apache.spark.sql.Row] = Array([panda,0], [pink,3], [pirate,3], [panda,1], [pink,4])

or 
Query with where clause 
`sqlContext.sql("select * from pairs where Count !=3").show()`
```
+-----+-----+
| Name|Count|
+-----+-----+
|panda|    0|
|panda|    1|
| pink|    4|
+-----+-----+
```
   
In above procedure, steps 3 and 4 can be directly done on input RDD as below
 `val pairDF = input.map(rec => { var splitRec = rec.split(",") ; Pair(splitRec(0),splitRec(1).toInt) }).toDF`
pairDF: org.apache.spark.sql.DataFrame = [Name: string, Count: int]
 
Even printSchema can be done on pairDF
`pairDF.printSchema`

Output -
```
root
 |-- Name: string (nullable = true)
 |-- Count: integer (nullable = false)
```

To see the results do 
`pairDF.show()`
```
+------+-----+
|  Name|Count|
+------+-----+
| panda|    0|
|  pink|    3|
|pirate|    3|
| panda|    1|
|  pink|    4|
+------+-----+
```

### To apply filter (in Spark 1.6)
`pairDF.filter(pairDF("Name") === "panda").show()`
```
+-----+-----+
| Name|Count|
+-----+-----+
|panda|    0|
|panda|    1|
+-----+-----+
```

### To save the Dataframe as text file convert it to RDD using .rdd and then use saveAsTextFile option.
`pairDF.rdd.saveAsTextFile("file:///home/technocrafty/shalini/spark/output/pairDF")`

### If want to change the number of partitioners/reducers, repartition(<numofpartitions>) can be used  on rdd
pairDF.rdd.repartition(1).saveAsTextFile("file:///home/technocrafty/shalini/spark/output/pairDF")

### In order to change the number of partitions in Spark SQL. In Spark, repartition and coalesce work
`sqlContext.setConf("spark.sql.shuffle.partitions","1")`
 
###### Dataframe can also be saved without converting to rdd using a)save (Default saves as Parquet) b)saveAsParquetFile c)saveAsTable


### To read json file and print its schema (json automatically infer the schema of json file)
`val jsonDF = sqlContext.read.json("file:///home/technocrafty/shalini/spark/input/edcr_kpi_header.json")`

##### Following line print the schema oof json file
`jsonDF.printSchema`

#### Select single column  and print using show
`jsonDF.select("header.icid") .show`

#### Filter records based on certain criteria and print it using select and show
`jsonDF.filter(jsonDF("header.legs") > 1).select("header.legs","header.icid") .show`

#### Group by
`jsonDF.groupBy("header.legs").count().show`

#### Select & Print multiple columns
`jsonDF.select("header.icid","header.legs").show`

#### To Run sql query register the DF as temp table and then query on it
`jsonDF.registerTempTable("edcr_kpi_header")` \
`val dataFM = sqlContext.sql("SELECT * FROM edcr_kpi_header")` \
`dataFM.show` \

`sqlContext.sql("SELECT header.legs as legs,count(*) as count FROM edcr_kpi_header group by header.legs").show`


### Creating and reading parquet files
-------------------------------------------
`import sqlContext.implicits._`

Create emplpyee RDD using any file or manually \
`val employee =  sc.textFile("file:///home/technocrafty/shalini/spark/input/empsalary.txt")`

Create Employee case class
`case class Employee (id:Int,name:String,age:Int,gender:String,salary:Int)`

Map employee data to EmployeeRDD
```
val empRDD = employee.map(e => { 
val splitrec = e.split("\\t"); 
Employee(splitrec(0).toInt,splitrec(1),splitrec(2).toInt,splitrec(3),splitrec(4).toInt)
})
```
Save empRDD as DF to parquet file
`empRDD.toDF().write.parquet("file:///home/technocrafty/shalini/spark/input/employee.parquet")`

Now use above created parquet file for querying
`val parquetFileRead  = sqlContext.read.parquet("file:///home/technocrafty/shalini/spark/input/employee.parquet")`
`parquetFileRead.registerTempTable("employees")`

`val teenagers = sqlContext.sql("SELECT * FROM employees where age < 20")`
`teenagers.show`

### Creating Partition by specifying path and merging schema
-----------------------------------------------------------------
1. create random DF
`val df1 = sc.makeRDD(1 to 5).map(x => (x,x*2)).toDF("single","double")`

2. create another random DF with atleast one column as same as above DF
`val df2 = sc.makeRDD(6 to 10).map(x => (x,x*3)).toDF("single","triple")`

3. Save both the DF as parrquet in specified partition location (Partition is key=1 for df1 and key=2 for df2)
`df1.write.parquet("file:///home/technocrafty/shalini/spark/input/table/key=1")` \
`df2.write.parquet("file:///home/technocrafty/shalini/spark/input/table/key=2")`

4. Merge both the DF using mergeSchema as true option

`val df3 = sqlContext.read.option("mergeSchema","true").parquet("file:///home/technocrafty/shalini/spark/input/table")`

5. Now printSchema on merged DF and see the output it shows the common column only once. Also, it shows partitioned column and its datatype
`df3.printSchema`
Output is :::
```
root
 |-- single: integer (nullable = true)
 |-- triple: integer (nullable = true)
 |-- double: integer (nullable = true)
 |-- key: integer (nullable = true)
 ```
 
6. Query the merged schema and see output

`df3.registerTempTable("df3")` \
`sqlContext.sql("SELECT * FROM df3").show`

Output :::
```
+------+------+------+---+
|single|triple|double|key|
+------+------+------+---+
|     1|  null|     2|  1|
|     2|  null|     4|  1|
|     3|  null|     6|  1|
|     4|  null|     8|  1|
|     5|  null|    10|  1|
|     6|    18|  null|  2|
|     7|    21|  null|  2|
|     8|    24|  null|  2|
|     9|    27|  null|  2|
|    10|    30|  null|  2|
+------+------+------+---+
```

FilteredQuery : `sqlContext.sql("SELECT * FROM df3 where key = 1 order by single").show`
Output :::
```
+------+------+------+---+
|single|triple|double|key|
+------+------+------+---+
|     1|  null|     2|  1|
|     2|  null|     4|  1|
|     3|  null|     6|  1|
|     4|  null|     8|  1|
|     5|  null|    10|  1|
+------+------+------+---+
```

### Join query
`val df1 = sc.makeRDD(1 to 5).map(x => (x,x*2)).toDF("single","double")` \
`val df2 = sc.makeRDD(4 to 10).map(x => (x,x*3)).toDF("single","triple")` \
`df1.registerTempTable("df1")` \
`df2.registerTempTable("df2")` \
`df1.show`
```
+------+------+
|single|double|
+------+------+
|     1|     2|
|     2|     4|
|     3|     6|
|     4|     8|
|     5|    10|
+------+------+
```
`df2.show`
```
+------+------+
|single|triple|
+------+------+
|     4|    12|
|     5|    15|
|     6|    18|
|     7|    21|
|     8|    24|
|     9|    27|
|    10|    30|
+------+------+
```

`sqlContext.sql("SELECT df1.single,df1.double,df2.triple from df1 ,df2 where df1.single = df2.single").show`
```
+------+------+------+
|single|double|triple|
+------+------+------+
|     4|     8|    12|
|     5|    10|    15|
+------+------+------+
```

### To create jsonRDD and read it and query on it
-------------------------------------------------------
**Step 1** Create jsondata using `sc.parallelize` \
`val jsondata = sc.parallelize("""{"header":{"icid":"sgc1.cratf001.sip.t-mobile.com-1493-870625-208367","legs":2,"type":"CALL","start":1493870625257,"dur":54708,"subProcedures":["audio"],"end":1493870700592},"dimensions":{"mo":{"msisdn":436656260039917,"imsi":310260623187599,"imeitac":86092603,"imeisvn":9,"src_addr":"10.164.126.4","dest_addr":"10.161.186.5","user_agent":"T-Mobile VoLTE Qualcomm-IMS-client/3.0","uplane_gw_addr":"fd00:976a:14f9:193e::4"},"mt":{"msisdn":18562260825937,"imsi":310260507825809,"imeitac":86092603,"imeisvn":9,"src_addr":"10.174.10.236","dest_addr":"10.163.225.13","user_agent":"T-Mobile VoLTE Qualcomm-IMS-client/3.0"}}}""" :: Nil)`

**Step 2** Read jsondata RDD instead of json file using sqlContext.read.json
`val jsonRDD = sqlContext.read.json(jsondata)`

**Step 3** Convert to DF and printSchema
`val jsonDF = jsonRDD.toDF()` \
`jsonDF.printSchema` \
`jsonDF.show` \
Output ::
```
+--------------------+--------------------+
|          dimensions|              header|
+--------------------+--------------------+
|[[10.161.186.5,9,...|[54708,1493870700...|
+--------------------+--------------------+
```

`jsonDF.foreach(println)` \
Output  ::
```
[[[10.161.186.5,9,86092603,310260623187599,436656260039917,10.164.126.4,fd00:976a:14f9:193e::4,T-Mobile VoLTE Qualcomm-IMS-client/3.0],[10.163.225.13,9,86092603,310260507825809,18562260825937,10.174.10.236,T-Mobile VoLTE Qualcomm-IMS-client/3.0]],[54708,1493870700592,sgc1.cratf001.sip.t-mobile.com-1493-870625-208367,2,1493870625257,List(audio),CALL]]
```

To Query on it,register `jsonDF.registerTempTable("jsondf")`
and 
`sqlContext.sql("select header.icid as icid, header.type as type,dimensions.mo.msisdn as mo_msisdn,dimensions.mt.msisdn as mt_msisdn from jsondf").show` \
Output ::
```
+--------------------+----+---------------+--------------+
|                icid|type|      mo_msisdn|     mt_msisdn|
+--------------------+----+---------------+--------------+
|sgc1.cratf001.sip...|CALL|436656260039917|18562260825937|
+--------------------+----+---------------+--------------+
```

### Reading CSV File from HDFS and creating DF out of it & Performing operations on DF
Create case class to map input file structure \
`case class YahooStock(date:String,open_price:Float,high_price:Float,low_price:Float,close_price:Float,volume:Int,adj_price:Float)`

#### Read Input CSV File just like Text files \
`val inputRDD =  sc.textFile("/user/sparkuser/myspark/data/input/yahoo_stocks.csv")`

#### Remove header form the data using any of the following
`val headerRemoved = inputRDD.filter(x => !(x.split(",")(0).equals("Date")))`
or
`val headerRemoved = inputRDD.filter(x => !(x.startsWith("Date")))`

After removing the header map the filter data to case class YahooStock as per correct datatypes
```
val yahooStockDF = headerRemoved.map(x => 
                           { 
                           val columns = x.split(",") ;
                           YahooStock(columns(0),columns(1).toFloat,columns(2).toFloat,columns(3).toFloat,
                           columns(4).toFloat,columns(5).toInt,columns(6).toFloat)
                            }
                       ).toDF()
```

#### Validate the schema of the dataframe with that of case class using printSchema
`yahooStockDF.printSchema` \
O/P
```
root
 |-- date: string (nullable = true)
 |-- open_price: float (nullable = false)
 |-- high_price: float (nullable = false)
 |-- low: float (nullable = false)
 |-- close: float (nullable = false)
 |-- Volume: integer (nullable = false)
 |-- adj_close: float (nullable = false)
 ```

#### Selecting maximum value of opening stock from Data Frame
`yahooStockDF.select(max(yahooStockDF("open_price"))).show`
OR
`yahooStockDF.select(max($"open_price")).show`
  
 #### Sorting Data by date field in ascending order
`val sortedByDateDF =  yahooStockDF.sort($"date") OR val sortedByDateDF =  yahooStockDF.sort(yahooStockDF("date"))`
O/P
```
sortedByDateDF: org.apache.spark.sql.DataFrame = [date: string, open_price: float, high_price: float, low_price: float, close_price: float, volume: int, adj_price: float]
```

#### Sorting Yahoo Stock DF by "open_price" in descending order
`val sortedByOpenDesc = yahooStockDF.sort($"open_price".desc)`

#### Printing 10 records of DF sorted by date
`sortedByOpenDesc.show(10)`
```
+----------+----------+----------+---------+-----------+---------+---------+
|      date|open_price|high_price|low_price|close_price|   volume|adj_price|
+----------+----------+----------+---------+-----------+---------+---------+
|2000-01-04|     464.5|   500.125|    442.0|      443.0| 69868800|   110.75|
|2000-01-03|  442.9218|     477.0|    429.5|      475.0| 38469600|   118.75|
|1999-01-12| 438.62497|     443.0|    370.0|      402.0|104092000|    50.25|
|2000-01-10|     432.5|    451.25|    420.0|  436.06247| 61022400|109.01562|
|2000-01-05|     430.5|   431.125|    402.0|      410.5| 83194800|  102.625|
|2000-01-11|   423.875|    426.25|    392.0|    397.375| 75761600| 99.34375|
|1999-12-30|    421.75|     448.0|   406.75|  416.06247| 24972400|104.01562|
|1999-12-31| 420.43753|     441.5|410.06247|  432.68753| 10116400|108.17188|
|1999-12-23|     417.5|    426.25|    400.0|    402.625| 18468400|100.65625|
|1999-12-28|     410.0|     420.0|    390.0|     390.25| 20896400|  97.5625|
+----------+----------+----------+---------+-----------+---------+---------+
```

#### To save output as Text file and partition numbers are reduced using coalesce(1). It will generate all output in single file
`sortedByDateDF.map(x=>x.mkString(",")).coalesce(1).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_stock/sortedByDate")`
 
##### Following line of code will generate output in 200 files as DF use 200 partitions by default and it will also take more time to finish the job as 200 tasks(each on 1 partition) will run 
`sortedByDateDF.map(x=>x.mkString(",")).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_stock/sortedByDate")`
 
###### To avoid above condition either use coalesce or set following property in sql context. It will use only single partition throughout
`sqlContext.setConf("spark.sql.shuffle.partitions","1")`
 
###### If we want to increase above value to 2 for our statement, we can also use repartition but if we use coalesce to increse the number of partitions, it will not increase, it will take older no. of partitions and hence generate only single output file.
`sortedByDateDF.map(x=>x.mkString(",")).coalesce(2).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_stock/sortedByDate")`
  
Now let us see by using repartition to increase the no of partitions. Yes, it works !!
`sortedByDateDF.map(x=>x.mkString(",")).repartition(2).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_stock/sortedByDate")`

###### Let us move back to coalesce to reduce the number of partitions. Yes, it works for reducing. Even repartition can also be used for reducing
`sortedByDateDF.map(x=>x.mkString(",")).coalesce(1).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_stock/sortedByDate")` \
`sortedByDateDF.map(x=>x.mkString(",")).repartition(1).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_stock/sortedByDate")`

  
#### In order to know the partitions of underlying rdd in Dataframe
`yahooStockDF.rdd.partitions.length`
  
###### Try to cache actual RDD and DF created and the see the storage occupied by both of them .It is lesser in case of DF.
  
### Registering Dataframe as table and then querying it
`yahooStockDF.registerTempTable("yahoo_stock")` \
`sqlContext.sql("SELECT * FROM yahoo_stock WHERE open_price > 40 AND high_price > 40 ORDER BY date ASC LIMIT 5").show` \
O/P:
```
+----------+----------+----------+---------+-----------+--------+---------+
|      date|open_price|high_price|low_price|close_price|  volume|adj_price|
+----------+----------+----------+---------+-----------+--------+---------+
|1997-07-09|  40.75008|  45.12504| 40.75008|   43.99992|37545600|  1.83333|
|1997-07-10|  45.37488|  46.12512| 41.74992|   43.00008|44035200|  1.79167|
|1997-07-11|  43.00008|  45.12504| 42.25008|   43.99992|15331200|  1.83333|
|1997-07-14|  43.99992|  48.49992| 43.87488|   48.49992|24980800|  2.02083|
|1997-07-15|  48.25008|  50.74992| 47.74992|   50.50008|33832000|  2.10417|
+----------+----------+----------+---------+-----------+--------+---------+
```


## Hive & SQLContext
### To see all databases in Hive
`sqlContext.sql("SHOW DATABASES").show`
+-------+
| result|
+-------+
|default|
| xademo|
+-------+

### To see all tables in Hive
`sqlContext.sql("SHOW TABLES IN xademo").show`
+-------------------+-----------+
|          tableName|isTemporary|
+-------------------+-----------+
|call_detail_records|      false|
|   customer_details|      false|
|   recharge_details|      false|
+-------------------+-----------+

### To see count of records in customer_details table
`sqlContext.sql("SELECT * FROM xademo.customer_details").count`
O/P
res1: Long = 30

#### Create retail_db database in Hive and use script from hive-create-retail_db.sql script to create  tables and load data in those table.
1. Launch spark-shell
 
2. Create hiveContext if sqlContext is not preloaded with Hive and use hiveContext instead of sqlContext
`val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);`

3. Check if Hive tables are available in sqlContext
`sqlContext.sql("SHOW TABLES IN retail_db").show`
OR 
if using hiveContext
`hiveContext.sql("SHOW TABLES IN retail_db").show`
O/P
```
+-----------+-----------+
|  tableName|isTemporary|
+-----------+-----------+
| categories|      false|
|  customers|      false|
|departments|      false|
|order_items|      false|
|     orders|      false|
|   products|      false|
+-----------+-----------+
```

#### Create Yahoo Stock ORC table
`sqlContext.sql("CREATE TABLE retail_db.yahoo_stock_orc(date STRING,open_price FLOAT,high_price FLOAT,low_price FLOAT,close_price FLOAT,volume INT,adj_price FLOAT) STORED AS ORC")`

#### Validate if table is created or not using 
`sqlContext.sql("SHOW TABLES IN retail_db").show`
```
+---------------+-----------+
|      tableName|isTemporary|
+---------------+-----------+
|     categories|      false|
|      customers|      false|
|    departments|      false|
|    order_items|      false|
|         orders|      false|
|       products|      false|
|yahoo_stock_orc|      false|
+---------------+-----------+
```

#### We can Load "/user/sparkuser/myspark/data/input/yahoo_stocks.csv" in table directly
`sqlContext.sql("LOAD DATA INPATH "/user/sparkuser/myspark/data/input/yahoo_stocks.csv" INTO TABLE yahoo_stock_orc")` \
OR 

1. Create inputRDD and then filter header row and load data into Hive use spark RDD & creating temporary table and loading from it.
`case class YahooStock(date:String,open_price:Float,high_price:Float,low_price:Float,close_price:Float,volume:Int,adj_price:Float)` \
`val inputRDD =  sc.textFile("/user/sparkuser/myspark/data/input/yahoo_stocks.csv")` \
`val headerRemoved = inputRDD.filter(x => !(x.startsWith("Date"))) \`
`headerRemoved.count`

```
val yahooStockDF = headerRemoved.map(x => { 
                             val columns = x.split(",") ;                
                             YahooStock(columns(0),columns(1).toFloat,columns(2).toFloat,columns(3).toFloat,
                             columns(4).toFloat,columns(5).toInt,columns(6).toFloat)
                             }).toDF()
 ```
`yahooStockDF.registerTempTable("yahoo_stock")`

`sqlContext.sql("select * from yahoo_stock").count` \
`sqlContext.sql("INSERT OVERWRITE TABLE retail_db.yahoo_stock_orc SELECT * FROM yahoo_stock")` \
`sqlContext.sql("SELECT * FROM retail_db.yahoo_stock_orc").count`  \

#### Validate the results of both yahoo_stock Temp table and retail_db.yahoo_stock_orc table
`sqlContext.sql("SELECT * FROM yahoo_stock WHERE open_price > 40 AND high_price > 40 ORDER BY date ASC LIMIT 20").show` \
`sqlContext.sql("SELECT * FROM retail_db.yahoo_stock_orc WHERE open_price > 40 AND high_price > 40 ORDER BY date ASC LIMIT 20").show`

#### Create a DF to save it as ORC and text file
`val topPriceResultsDF = sqlContext.sql("SELECT * FROM retail_db.yahoo_stock_orc WHERE open_price > 40 AND high_price > 40 ORDER BY date ASC")`

#### To add header to above query results before saving in text file
`val header:String = "date,open_price,high_price,low_price,close_price,volume,adj_price"`

##### Use mapPartitions if we want to add header in all files or if there is single partition.
`topPriceResultsDF.map(x => x.mkString(","))
.mapPartitions(iter => Iterator(header) ++ iter)
.saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_above40resultsWithHeader.csv")`
OR
##### Use mapPartitionsWithIndex  if we want to add header in only first file
`topPriceResultsDF.map(x => x.mkString(","))
.repartition(2)
.mapPartitionsWithIndex ({
case (0, iter) => Iterator(header) ++ iter
case (_, iter) => iter
}).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_above40resultsWithHeader.csv")`


#### Saving above DF or query results as Tab & Comma separated Text File
`topPriceResultsDF.map(x => x.mkString(",")).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_above40_results(comma).csv")` \ 
`topPriceResultsDF.map(x => x.mkString("\t")).saveAsTextFile("/user/sparkuser/myspark/data/output/yahoo_above40_results(tab).csv")` 

### Saving topPriceResultsDF with different delimiter for each partition (Assuming there are already 2 partitions done)
```
topPriceResultsDF.rdd.mapPartitionsWithIndex({
                   case(0,iter) => iter.map(x=>x.mkString(","))
                   case (1,iter) => iter.map(x=>x.mkString("|"))
 }).saveAsTextFile("/user/sparkuser/myspark/mappedDF")
 ```

#### Saving any DF or query results as ORC
`topPriceResultsDF.write.format("orc").save("/user/sparkuser/myspark/yahoo_above40_results_orc")`

#### Reading above created orc file "/user/sparkuser/myspark/yahoo_above40_results_orc" 
`val inputORCDF = sqlContext.read.format("orc").load("/user/sparkuser/myspark/yahoo_above40_results_orc")`
O/P
```
17/08/10 05:30:10 INFO OrcRelation: Listing hdfs://sandbox.hortonworks.com:8020/user/sparkuser/myspark/yahoo_above40_results_orc on driver
inputORCDF: org.apache.spark.sql.DataFrame = [date: string, open_price: float, high_price: float, low_price: float, close_price: float, volume: int, adj_price: float]
```

`inputORCDF.show(20)`
O/P
```
+----------+----------+----------+---------+-----------+--------+---------+
|      date|open_price|high_price|low_price|close_price|  volume|adj_price|
+----------+----------+----------+---------+-----------+--------+---------+
|1997-07-09|  40.75008|  45.12504| 40.75008|   43.99992|37545600|  1.83333|
|1997-07-10|  45.37488|  46.12512| 41.74992|   43.00008|44035200|  1.79167|
|1997-07-11|  43.00008|  45.12504| 42.25008|   43.99992|15331200|  1.83333|
|1997-07-14|  43.99992|  48.49992| 43.87488|   48.49992|24980800|  2.02083|
|1997-07-15|  48.25008|  50.74992| 47.74992|   50.50008|33832000|  2.10417|
|1997-07-16|  51.25008|  51.25008| 49.00008|   49.12512|11449600|  2.04688|
|1997-07-17|  49.12512|  49.12512|     46.5|   46.87488|12688000|  1.95312|
|1997-07-18|  46.37496|  48.37488| 44.87496|   46.87488|12059200|  1.95312|
|1997-07-21|  47.50008|  49.99992| 46.99992|   49.62504|11200000|  2.06771|
|1997-07-22|  49.75008|  51.12504| 48.25008|   50.43744|11822400|  2.10156|
|1997-07-23|  50.74992|  50.74992| 49.12512|   49.12512| 7364800|  2.04688|
|1997-07-24|  49.00008|      49.5| 46.75008|   47.50008|15115200|  1.97917|
|1997-07-25|  47.74992|  48.12504| 46.18752|   47.87496| 9232000|  1.99479|
|1997-07-28|  48.25008|  49.00008| 47.50008|   47.56248| 3806400|  1.98177|
|1997-07-29|  47.62512|  48.25008| 46.99992|   48.25008| 4523200|  2.01042|
|1997-07-30|  49.75008|  55.00008| 49.62504|   54.62496|47521600|  2.27604|
|1997-07-31|  55.00008|  56.50008| 52.37496|   56.50008|33768000|  2.35417|
|1997-08-01|  55.99992|  55.99992| 53.37504|   55.24992|19910400|  2.30208|
|1997-08-04|     54.75|     54.75| 53.12496|   53.37504|12841600|  2.22396|
|1997-08-05|  53.74992|      55.5|    53.25|   54.37488|11304000|  2.26562|
+----------+----------+----------+---------+-----------+--------+---------+
only showing top 20 rows
```

#### Using same ORC file /user/sparkuser/myspark/yahoo_above40_results_orc) to create EXTERNAL ORC Table
`sqlContext.sql("CREATE EXTERNAL TABLE retail_db.yahoo_above40_results_orc(date STRING,open_price FLOAT,high_price FLOAT,low_price FLOAT,close_price FLOAT,volume INT,adj_price FLOAT) STORED AS ORC LOCATION '/user/sparkuser/myspark/yahoo_above40_results_orc'")`

#### Use following command to validate if data is coming correctly or not
`sqlContext.sql("SELECT * FROM retail_db.yahoo_above40_results_orc").show(10)`
O/P
```
+----------+----------+----------+---------+-----------+--------+---------+
|      date|open_price|high_price|low_price|close_price|  volume|adj_price|
+----------+----------+----------+---------+-----------+--------+---------+
|1997-07-09|  40.75008|  45.12504| 40.75008|   43.99992|37545600|  1.83333|
|1997-07-10|  45.37488|  46.12512| 41.74992|   43.00008|44035200|  1.79167|
|1997-07-11|  43.00008|  45.12504| 42.25008|   43.99992|15331200|  1.83333|
|1997-07-14|  43.99992|  48.49992| 43.87488|   48.49992|24980800|  2.02083|
|1997-07-15|  48.25008|  50.74992| 47.74992|   50.50008|33832000|  2.10417|
|1997-07-16|  51.25008|  51.25008| 49.00008|   49.12512|11449600|  2.04688|
|1997-07-17|  49.12512|  49.12512|     46.5|   46.87488|12688000|  1.95312|
|1997-07-18|  46.37496|  48.37488| 44.87496|   46.87488|12059200|  1.95312|
|1997-07-21|  47.50008|  49.99992| 46.99992|   49.62504|11200000|  2.06771|
|1997-07-22|  49.75008|  51.12504| 48.25008|   50.43744|11822400|  2.10156|
+----------+----------+----------+---------+-----------+--------+---------+
```

#### Use following query to check that there are no records with open_price < 40 or  high_price < 40
`sqlContext.sql("SELECT * FROM retail_db.yahoo_above40_results_orc WHERE open_price < 40 OR high_price < 40").show()`
O/P
+----+----------+----------+---------+-----------+------+---------+
|date|open_price|high_price|low_price|close_price|volume|adj_price|
+----+----------+----------+---------+-----------+------+---------+
+----+----------+----------+---------+-----------+------+---------+


#### We can also validate the table metadata using below command . Note false is used in show() for not to truncate the column results.
sqlContext.sql("DESCRIBE FORMATTED retail_db.yahoo_above40_results_orc").show(false)
O/P
```
+-----------------------------------------------------------------------------------------------------------+
|result                                                                                                     |
+-----------------------------------------------------------------------------------------------------------+
|# col_name             data_type               comment                                                        |
|                                                                                                                       |
|date                   string                                                                                   |
|open_price             float                                                                                    |
|high_price             float                                                                                    |
|low_price              float                                                                                    |
|close_price            float                                                                                    |
|volume                 int                                                                                      |
|adj_price              float                                                                                    |
|                                                                                                                       |
|# Detailed Table Information                                                                                       |
|Database:              retail_db                                                                                |
|Owner:                 root                                                                                     |
|CreateTime:            Thu Aug 10 05:48:32 UTC 2017                                                             |
|LastAccessTime:        UNKNOWN                                                                                  |
|Protect Mode:          None                                                                                     |
|Retention:             0                                                                                        |
|Location:              hdfs://sandbox.hortonworks.com:8020/user/sparkuser/myspark/yahoo_above40_results_orc     |
|Table Type:            EXTERNAL_TABLE                                                                           |
|Table Parameters:                                                                                                     |
+-----------------------------------------------------------------------------------------------------------+
```

`sqlContext.sql("SHOW CREATE TABLE  retail_db.yahoo_above40_results_orc").show(false)`
O/P
```
+----------------------------------------------------------------------------------------+
|result                                                                                  |
+----------------------------------------------------------------------------------------+
|CREATE EXTERNAL TABLE `retail_db.yahoo_above40_results_orc`(                            |
|  `date` string,                                                                        |
|  `open_price` float,                                                                   |
|  `high_price` float,                                                                   |
|  `low_price` float,                                                                    |
|  `close_price` float,                                                                  |
|  `volume` int,                                                                         |
|  `adj_price` float)                                                                    |
|ROW FORMAT SERDE                                                                        |
|  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'                                           |
|STORED AS INPUTFORMAT                                                                   |
|  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'                                     |
|OUTPUTFORMAT                                                                            |
|  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'                                    |
|LOCATION                                                                                |
|  'hdfs://sandbox.hortonworks.com:8020/user/sparkuser/myspark/yahoo_above40_results_orc'|
|TBLPROPERTIES (                                                                         |
|  'COLUMN_STATS_ACCURATE'='false',                                                      |
|  'numFiles'='1',                                                                       |
|  'numRows'='-1',                                                                       |
+----------------------------------------------------------------------------------------+
```

#### Create a table in Hive using tab separated file
`sqlContext.sql("CREATE TABLE emp_salary(id SMALLINT,name STRING,age TINYINT,gender STRING,salary INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'") `

#### Validate the metadata of table
`sqlContext.sql("SHOW CREATE TABLE emp_salary").show(false)`
O/P
```
+----------------------------------------------------------------------+
|result                                                                |
+----------------------------------------------------------------------+
|CREATE TABLE `emp_salary`(                                            |
|  `id` smallint,                                                      |
|  `name` string,                                                      |
|  `age` tinyint,                                                      |
|  `gender` string,                                                    |
|  `salary` int)                                                       |
|ROW FORMAT DELIMITED                                                  |
|  FIELDS TERMINATED BY '\t'                                           |
|STORED AS INPUTFORMAT                                                 |
|  'org.apache.hadoop.mapred.TextInputFormat'                          |
|OUTPUTFORMAT                                                          |
|  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'        |
|LOCATION                                                              |
|  'hdfs://sandbox.hortonworks.com:8020/apps/hive/warehouse/emp_salary'|
|TBLPROPERTIES (                                                       |
|  'transient_lastDdlTime'='1502446320')                               |
+----------------------------------------------------------------------+
```

#### Load Data in above  tab separated file
`sqlContext.sql("LOAD DATA INPATH '/user/sparkuser/myspark/data/input/empsalary.txt' INTO TABLE emp_salary")`

#### Validate Data by quering
`sqlContext.sql("SELECT * FROM emp_salary").show(5)`
O/P
```
+----+-------+---+------+------+
|  id|   name|age|gender|salary|
+----+-------+---+------+------+
|1201|  Gopal| 45|  Male| 50000|
|1202|manisha| 40|Female| 50000|
|1203| Khalil| 34|  Male| 30000|
|1204| prasan| 30|  Male| 30000|
|1205|  Kiran| 20|  Male| 40000|
+----+-------+---+------+------+
```

#### Find maximum , minimum & average salary  from the above table
`sqlContext.sql("SELECT MAX(salary) AS MaxSalary,MIN(SALARY) AS MinSalary,ROUND(AVG(salary),2) AS AvgSalary FROM emp_salary").show`
O/P
```
+---------+---------+---------+
|MaxSalary|MinSalary|AvgSalary|
+---------+---------+---------+
|    50000|     8000| 28076.92|
+---------+---------+---------+
```

#### To calculate salary aggregations by gender
 `sqlContext.sql("SELECT gender, SUM(salary) AS total, COUNT(*) AS no, AVG(salary) as avg, MAX(salary) AS max, MIN(salary) as min from emp_salary GROUP BY gender").show`
 O/P
 ```
+------+------+---+-------+-----+-----+
|gender| total| no|    avg|  max|  min|
+------+------+---+-------+-----+-----+
|Female|123000|  5|24600.0|50000| 8000|
|  Male|242000|  8|30250.0|50000|20000|
+------+------+---+-------+-----+-----+
```


#### To calculate  average revenue of "COMPLETE" status orders by each date using retail_db Database in Hive
`val completedOrdersDF  = sqlContext.sql("SELECT o.order_date as date,order_id FROM retail_db.orders o WHERE o.order_status == 'COMPLETE'")`

`completedOrdersDF.registerTempTable("orders")`

`val ordersAvgRevenueByDate  = sqlContext.sql("SELECT o.date as OrderDate, AVG(order_item_subtotal) as AvgRevenue FROM orders o JOIN retail_db.order_items oi ON (o.order_id = oi.order_item_order_id) GROUP BY o.date ORDER BY o.date ASC")`

`val header:String = "Order Date,Average Revenue";`

`ordersAvgRevenueByDate.map(x => x.mkString(",")).mapPartitionsWithIndex({case (0,iter) => Iterator(header) ++ iter; case(_,iter) => iter;}).saveAsTextFile("/user/sparkuser/myspark/data/output/OrdersAvgRevenueSorted.csv")`

### #ERRORS CALCULATION AT EACH SEVERITY LEVEL IN LOG FILES ####################

#### Input File C:\MY_DRIVE\BigData`N`Hadoop\Certification\Spark\logs.txt##
```
INFO This is a message with content
INFO This is some other content
(empty line)
INFO Here are more messages
WARN This is a warning
(empty line)
ERROR Something bad happened
WARN More details on the bad thing
INFO back to normal messages
```

#### Solving using Spark SQL Application
```
spark-submit \
--master yarn-client \
--executor-memory 512m \
--num-executors 1 \
--conf spark.sql.shuffle.partitions=1 \
--total-executor-cores 2 \
--name "LogDemoWithSQL-4" \
--class myapps.LogDemoWithSQL \
/home/sparkuser/shalini/simple-app_2.10-1.0.jar \
/user/sparkuser/myspark/data/input/logs.txt \
/user/sparkuser/myspark/data/output/LogDemoWithSQL.txt
```
Above command will generate single file as partitions set are 1

```
spark-submit \
--master yarn-client \
--executor-memory 512m \
--num-executors 1 \
--total-executor-cores 1 \
--name "LogDemoWithoutAcc-1" \
--class myapps.LogDemoWithoutAcc \
/home/sparkuser/shalini/simple-app_2.10-1.0.jar \
/user/sparkuser/myspark/data/input/logs.txt \
/user/sparkuser/myspark/data/output/LogDemoWithoutAcc.txt
```
Above command will generate two file as partitions set are = no of cores (1 driver & 1 executor core set)

```
spark-submit \
--master yarn-cluster \
--executor-memory 512m \
--num-executors 1 \
--total-executor-cores 1 \
--name "LogDemoWithAcc-1" \
--class myapps.LogDemoWithAcc \
/home/sparkuser/shalini/simple-app_2.10-1.0.jar \
/user/sparkuser/myspark/data/input/logs.txt \
/user/sparkuser/myspark/data/output/LogDemoWithAcc.txt
```
Above command should have generated two file as partitions set are = no of cores (1 driver & 1 executor core set) but it generates 1 because of repartition used.

### To check running applications in YARN
`yarn application -list`

### To kill any application with application id in YARN
`yarn application -kill application_1502272592377_0003`


### SPARK SQL & JSON FILE

#### Read a json File
`val jsonDF = sqlContext.read.json("/user/sparkuser/myspark/data/input/edcr_kpi_long.json")`

#### Print the schema of the json
jsonDf.printSchema
```
root
 |-- call_kpis: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- dimensions: struct (nullable = true)
 |    |    |    |-- cause: string (nullable = true)
 |    |    |    |-- code: string (nullable = true)
 |    |    |-- dir: string (nullable = true)
 |    |    |-- leg: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- value: double (nullable = true)
 |-- dimensions: struct (nullable = true)
 |    |-- mo: struct (nullable = true)
 |    |    |-- dest_addr: string (nullable = true)
 |    |    |-- imeisvn: long (nullable = true)
 |    |    |-- imeitac: long (nullable = true)
 |    |    |-- imsi: long (nullable = true)
 |    |    |-- msisdn: long (nullable = true)
 |    |    |-- src_addr: string (nullable = true)
 |    |    |-- terminal_type: string (nullable = true)
 |    |    |-- uplane_gw_addr: string (nullable = true)
 |    |    |-- user_agent: string (nullable = true)
 |    |-- mt: struct (nullable = true)
 |    |    |-- dest_addr: string (nullable = true)
 |    |    |-- imeisvn: long (nullable = true)
 |    |    |-- imeitac: long (nullable = true)
 |    |    |-- imsi: long (nullable = true)
 |    |    |-- msisdn: long (nullable = true)
 |    |    |-- src_addr: string (nullable = true)
 |    |    |-- terminal_type: string (nullable = true)
 |    |    |-- uplane_gw_addr: string (nullable = true)
 |    |    |-- user_agent: string (nullable = true)
 |-- header: struct (nullable = true)
 |    |-- dur: long (nullable = true)
 |    |-- end: long (nullable = true)
 |    |-- icid: string (nullable = true)
 |    |-- legs: long (nullable = true)
 |    |-- start: long (nullable = true)
 |    |-- subProcedures: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- type: string (nullable = true)
 |-- incidents: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- cause_description: string (nullable = true)
 |    |    |-- is_triggered: boolean (nullable = true)
 |    |    |-- name: long (nullable = true)
 |    |    |-- reason: struct (nullable = true)
 |    |    |    |-- muted: string (nullable = true)
 |    |    |    |-- poor media: string (nullable = true)
 |    |    |    |-- sip_tcd: string (nullable = true)
 |-- mo: struct (nullable = true)
 |    |-- events: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- content: struct (nullable = true)
 |    |    |    |    |-- 3gpp_cause_group: string (nullable = true)
 |    |    |    |    |-- Direction: string (nullable = true)
 |    |    |    |    |-- ProcedureType: string (nullable = true)
 |    |    |    |    |-- SubProcedureType: string (nullable = true)
 |    |    |    |    |-- ackTS: long (nullable = true)
 |    |    |    |    |-- audio_codec_name: string (nullable = true)
 |    |    |    |    |-- audio_codec_profile: long (nullable = true)
 |    |    |    |    |-- audio_codec_sampling_rate: long (nullable = true)
 |    |    |    |    |-- ave_pwr_restriction: double (nullable = true)
 |    |    |    |    |-- ave_ul_pucch_sinr: double (nullable = true)
 |    |    |    |    |-- ave_ul_pusch_sinr: double (nullable = true)
 |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |-- bearers: struct (nullable = true)
 |    |    |    |    |    |-- 5: struct (nullable = true)
 |    |    |    |    |    |    |-- addr: string (nullable = true)
 |    |    |    |    |    |    |-- apn: string (nullable = true)
 |    |    |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- enb_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- enb_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- max_dl: long (nullable = true)
 |    |    |    |    |    |    |-- max_ul: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- pci: long (nullable = true)
 |    |    |    |    |    |    |-- pl: long (nullable = true)
 |    |    |    |    |    |    |-- pvi: long (nullable = true)
 |    |    |    |    |    |    |-- qci: long (nullable = true)
 |    |    |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |    |    |-- thp: long (nullable = true)
 |    |    |    |    |    |    |-- traffic_class: string (nullable = true)
 |    |    |    |    |    |    |-- v6_addr: string (nullable = true)
 |    |    |    |    |    |-- 6: struct (nullable = true)
 |    |    |    |    |    |    |-- apn: string (nullable = true)
 |    |    |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- enb_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- enb_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- max_dl: long (nullable = true)
 |    |    |    |    |    |    |-- max_ul: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- qci: long (nullable = true)
 |    |    |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |    |    |-- thp: long (nullable = true)
 |    |    |    |    |    |    |-- traffic_class: string (nullable = true)
 |    |    |    |    |    |    |-- v6_addr: string (nullable = true)
 |    |    |    |    |    |-- 7: struct (nullable = true)
 |    |    |    |    |    |    |-- apn: string (nullable = true)
 |    |    |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- enb_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- enb_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- max_dl: long (nullable = true)
 |    |    |    |    |    |    |-- max_ul: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- pci: long (nullable = true)
 |    |    |    |    |    |    |-- pl: long (nullable = true)
 |    |    |    |    |    |    |-- pvi: long (nullable = true)
 |    |    |    |    |    |    |-- qci: long (nullable = true)
 |    |    |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |    |    |-- thp: long (nullable = true)
 |    |    |    |    |    |    |-- v6_addr: string (nullable = true)
 |    |    |    |    |    |-- 8: struct (nullable = true)
 |    |    |    |    |    |    |-- apn: string (nullable = true)
 |    |    |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- enb_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- enb_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- qci: long (nullable = true)
 |    |    |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |    |    |-- thp: long (nullable = true)
 |    |    |    |    |    |    |-- v6_addr: string (nullable = true)
 |    |    |    |    |-- callId: string (nullable = true)
 |    |    |    |    |-- cancelTS: long (nullable = true)
 |    |    |    |    |-- causeCode: long (nullable = true)
 |    |    |    |    |-- causeDescription: string (nullable = true)
 |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |-- cid: long (nullable = true)
 |    |    |    |    |-- codec_name: string (nullable = true)
 |    |    |    |    |-- codec_sampling_rate: long (nullable = true)
 |    |    |    |    |-- cqi: double (nullable = true)
 |    |    |    |    |-- csfb: boolean (nullable = true)
 |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |-- delay: double (nullable = true)
 |    |    |    |    |-- delay_since_last_report: double (nullable = true)
 |    |    |    |    |-- destinationAddressV4: string (nullable = true)
 |    |    |    |    |-- down: struct (nullable = true)
 |    |    |    |    |    |-- bad_frames: long (nullable = true)
 |    |    |    |    |    |-- codec_name: string (nullable = true)
 |    |    |    |    |    |-- codec_rate: double (nullable = true)
 |    |    |    |    |    |-- lost_frames: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_jumpback: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_out_of_order: long (nullable = true)
 |    |    |    |    |    |-- noise_packets: long (nullable = true)
 |    |    |    |    |    |-- packets: long (nullable = true)
 |    |    |    |    |    |-- payload_bytes: long (nullable = true)
 |    |    |    |    |    |-- sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |    |-- sequence_restarts: long (nullable = true)
 |    |    |    |    |    |-- speech_packets: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |-- eci: long (nullable = true)
 |    |    |    |    |-- enb_name: string (nullable = true)
 |    |    |    |    |-- enbid: long (nullable = true)
 |    |    |    |    |-- enbs1apid: long (nullable = true)
 |    |    |    |    |-- finalAnswerAddressV4: string (nullable = true)
 |    |    |    |    |-- finalAnswerTS: long (nullable = true)
 |    |    |    |    |-- fromAudioPort: long (nullable = true)
 |    |    |    |    |-- fromImsi: long (nullable = true)
 |    |    |    |    |-- fromMsisdn: long (nullable = true)
 |    |    |    |    |-- from_user_agent: string (nullable = true)
 |    |    |    |    |-- gummei: string (nullable = true)
 |    |    |    |    |-- gw_addr6: string (nullable = true)
 |    |    |    |    |-- gw_port: long (nullable = true)
 |    |    |    |    |-- hoExecInResult: string (nullable = true)
 |    |    |    |    |-- hoExecOutResult: string (nullable = true)
 |    |    |    |    |-- hoType: string (nullable = true)
 |    |    |    |    |-- icid: string (nullable = true)
 |    |    |    |    |-- imeisvn: long (nullable = true)
 |    |    |    |    |-- imeitac: long (nullable = true)
 |    |    |    |    |-- initiated_by_command: boolean (nullable = true)
 |    |    |    |    |-- inviteTS: long (nullable = true)
 |    |    |    |    |-- is_enbs1apid: boolean (nullable = true)
 |    |    |    |    |-- is_mmes1apid: boolean (nullable = true)
 |    |    |    |    |-- jitter: double (nullable = true)
 |    |    |    |    |-- mbCause: long (nullable = true)
 |    |    |    |    |-- mcc: long (nullable = true)
 |    |    |    |    |-- mme_ipv4_addr: string (nullable = true)
 |    |    |    |    |-- mme_teid: long (nullable = true)
 |    |    |    |    |-- mmes1apid: long (nullable = true)
 |    |    |    |    |-- mnc: long (nullable = true)
 |    |    |    |    |-- pdcp_loss_dl: long (nullable = true)
 |    |    |    |    |-- pdcp_loss_ul: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_lost_ho_dl: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_lost_perf_dl: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_lost_ul: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_rec_dl: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_rec_ul: long (nullable = true)
 |    |    |    |    |-- pgw_s5s8_ipv4_addr: string (nullable = true)
 |    |    |    |    |-- pgw_s5s8_teid: long (nullable = true)
 |    |    |    |    |-- pkt_loss: long (nullable = true)
 |    |    |    |    |-- prepCause: boolean (nullable = true)
 |    |    |    |    |-- reason: string (nullable = true)
 |    |    |    |    |-- report_config_type: string (nullable = true)
 |    |    |    |    |-- result: string (nullable = true)
 |    |    |    |    |-- ringingTS: long (nullable = true)
 |    |    |    |    |-- rsrp: long (nullable = true)
 |    |    |    |    |-- rsrq: double (nullable = true)
 |    |    |    |    |-- rtpDirection: string (nullable = true)
 |    |    |    |    |-- s1_release_cause: string (nullable = true)
 |    |    |    |    |-- sgw_ipv4_addr: string (nullable = true)
 |    |    |    |    |-- sgw_teid: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_0: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_1: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_2: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_3: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_4: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_5: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_6: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_7: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_0: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_1: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_2: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_3: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_4: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_5: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_6: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_7: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_8: long (nullable = true)
 |    |    |    |    |-- sourceAddressV4: string (nullable = true)
 |    |    |    |    |-- source_eci: long (nullable = true)
 |    |    |    |    |-- start: long (nullable = true)
 |    |    |    |    |-- stream_type: string (nullable = true)
 |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |-- successfulProcedure: boolean (nullable = true)
 |    |    |    |    |-- t3gpp_cause: long (nullable = true)
 |    |    |    |    |-- t_last_cqi: long (nullable = true)
 |    |    |    |    |-- t_last_meas: long (nullable = true)
 |    |    |    |    |-- tac: long (nullable = true)
 |    |    |    |    |-- target_eci: long (nullable = true)
 |    |    |    |    |-- tbs_pwr_restricted: long (nullable = true)
 |    |    |    |    |-- tbs_pwr_unrestricted: long (nullable = true)
 |    |    |    |    |-- term_addr6: string (nullable = true)
 |    |    |    |    |-- term_port: long (nullable = true)
 |    |    |    |    |-- toAudioPort: long (nullable = true)
 |    |    |    |    |-- toImsi: long (nullable = true)
 |    |    |    |    |-- toMsisdn: long (nullable = true)
 |    |    |    |    |-- to_user_agent: string (nullable = true)
 |    |    |    |    |-- up: struct (nullable = true)
 |    |    |    |    |    |-- bad_frames: long (nullable = true)
 |    |    |    |    |    |-- codec_name: string (nullable = true)
 |    |    |    |    |    |-- codec_rate: double (nullable = true)
 |    |    |    |    |    |-- lost_frames: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_jumpback: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_out_of_order: long (nullable = true)
 |    |    |    |    |    |-- noise_packets: long (nullable = true)
 |    |    |    |    |    |-- packets: long (nullable = true)
 |    |    |    |    |    |-- payload_bytes: long (nullable = true)
 |    |    |    |    |    |-- sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |    |-- sequence_restarts: long (nullable = true)
 |    |    |    |    |    |-- speech_packets: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |-- user_ipv6_addr: string (nullable = true)
 |    |    |    |    |-- validProcedure: boolean (nullable = true)
 |    |    |    |    |-- voice_integrity_dl: double (nullable = true)
 |    |    |    |    |-- voice_integrity_ul: double (nullable = true)
 |    |    |    |    |-- voice_mos: double (nullable = true)
 |    |    |    |-- header: struct (nullable = true)
 |    |    |    |    |-- highlighted: array (nullable = true)
 |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |-- loc_id: string (nullable = true)
 |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |-- ts: long (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |-- location_history: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- cellBeamDirection: double (nullable = true)
 |    |    |    |-- cellId: string (nullable = true)
 |    |    |    |-- cellLat: double (nullable = true)
 |    |    |    |-- cellLon: double (nullable = true)
 |    |    |    |-- cellName: string (nullable = true)
 |    |    |    |-- cellTechnology: string (nullable = true)
 |    |    |    |-- ts: long (nullable = true)
 |-- mt: struct (nullable = true)
 |    |-- events: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- content: struct (nullable = true)
 |    |    |    |    |-- 3gpp_cause_group: string (nullable = true)
 |    |    |    |    |-- Direction: string (nullable = true)
 |    |    |    |    |-- ProcedureType: string (nullable = true)
 |    |    |    |    |-- SubProcedureType: string (nullable = true)
 |    |    |    |    |-- ackTS: long (nullable = true)
 |    |    |    |    |-- audio_codec_name: string (nullable = true)
 |    |    |    |    |-- audio_codec_profile: long (nullable = true)
 |    |    |    |    |-- audio_codec_sampling_rate: long (nullable = true)
 |    |    |    |    |-- ave_pwr_restriction: double (nullable = true)
 |    |    |    |    |-- ave_ul_pucch_sinr: double (nullable = true)
 |    |    |    |    |-- ave_ul_pusch_sinr: double (nullable = true)
 |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |-- bearers: struct (nullable = true)
 |    |    |    |    |    |-- 5: struct (nullable = true)
 |    |    |    |    |    |    |-- apn: string (nullable = true)
 |    |    |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- enb_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- enb_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- max_dl: long (nullable = true)
 |    |    |    |    |    |    |-- max_ul: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- qci: long (nullable = true)
 |    |    |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |    |    |-- thp: long (nullable = true)
 |    |    |    |    |    |    |-- v6_addr: string (nullable = true)
 |    |    |    |    |    |-- 6: struct (nullable = true)
 |    |    |    |    |    |    |-- apn: string (nullable = true)
 |    |    |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- enb_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- enb_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- max_dl: long (nullable = true)
 |    |    |    |    |    |    |-- max_ul: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- qci: long (nullable = true)
 |    |    |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |    |    |-- thp: long (nullable = true)
 |    |    |    |    |    |    |-- v6_addr: string (nullable = true)
 |    |    |    |    |    |-- 7: struct (nullable = true)
 |    |    |    |    |    |    |-- apn: string (nullable = true)
 |    |    |    |    |    |    |-- bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |    |    |-- enb_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- enb_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- gbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- max_dl: long (nullable = true)
 |    |    |    |    |    |    |-- max_ul: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_dl: long (nullable = true)
 |    |    |    |    |    |    |-- mbr_ul: long (nullable = true)
 |    |    |    |    |    |    |-- pci: long (nullable = true)
 |    |    |    |    |    |    |-- pl: long (nullable = true)
 |    |    |    |    |    |    |-- pvi: long (nullable = true)
 |    |    |    |    |    |    |-- qci: long (nullable = true)
 |    |    |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_ipv4_addr: string (nullable = true)
 |    |    |    |    |    |    |-- sgw_up_teid: long (nullable = true)
 |    |    |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |    |    |-- thp: long (nullable = true)
 |    |    |    |    |    |    |-- v6_addr: string (nullable = true)
 |    |    |    |    |-- callId: string (nullable = true)
 |    |    |    |    |-- cancelTS: long (nullable = true)
 |    |    |    |    |-- causeCode: long (nullable = true)
 |    |    |    |    |-- causeDescription: string (nullable = true)
 |    |    |    |    |-- cause_value: long (nullable = true)
 |    |    |    |    |-- cid: long (nullable = true)
 |    |    |    |    |-- cqi: double (nullable = true)
 |    |    |    |    |-- csfb: boolean (nullable = true)
 |    |    |    |    |-- default_bearer_id: long (nullable = true)
 |    |    |    |    |-- delay: double (nullable = true)
 |    |    |    |    |-- destinationAddressV4: string (nullable = true)
 |    |    |    |    |-- down: struct (nullable = true)
 |    |    |    |    |    |-- bad_frames: long (nullable = true)
 |    |    |    |    |    |-- codec_name: string (nullable = true)
 |    |    |    |    |    |-- codec_rate: double (nullable = true)
 |    |    |    |    |    |-- lost_frames: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_jumpback: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_out_of_order: long (nullable = true)
 |    |    |    |    |    |-- noise_packets: long (nullable = true)
 |    |    |    |    |    |-- packets: long (nullable = true)
 |    |    |    |    |    |-- payload_bytes: long (nullable = true)
 |    |    |    |    |    |-- sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |    |-- sequence_restarts: long (nullable = true)
 |    |    |    |    |    |-- speech_packets: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |-- eci: long (nullable = true)
 |    |    |    |    |-- enb_name: string (nullable = true)
 |    |    |    |    |-- enbid: long (nullable = true)
 |    |    |    |    |-- enbs1apid: long (nullable = true)
 |    |    |    |    |-- finalAnswerAddressV4: string (nullable = true)
 |    |    |    |    |-- finalAnswerTS: long (nullable = true)
 |    |    |    |    |-- fromAudioPort: long (nullable = true)
 |    |    |    |    |-- fromImsi: long (nullable = true)
 |    |    |    |    |-- fromMsisdn: long (nullable = true)
 |    |    |    |    |-- from_user_agent: string (nullable = true)
 |    |    |    |    |-- gummei: string (nullable = true)
 |    |    |    |    |-- gw_addr6: string (nullable = true)
 |    |    |    |    |-- gw_port: long (nullable = true)
 |    |    |    |    |-- icid: string (nullable = true)
 |    |    |    |    |-- imeisvn: long (nullable = true)
 |    |    |    |    |-- imeitac: long (nullable = true)
 |    |    |    |    |-- initiated_by_command: boolean (nullable = true)
 |    |    |    |    |-- inviteTS: long (nullable = true)
 |    |    |    |    |-- is_enbs1apid: boolean (nullable = true)
 |    |    |    |    |-- is_mmes1apid: boolean (nullable = true)
 |    |    |    |    |-- mcc: long (nullable = true)
 |    |    |    |    |-- mme_ipv4_addr: string (nullable = true)
 |    |    |    |    |-- mme_teid: long (nullable = true)
 |    |    |    |    |-- mmes1apid: long (nullable = true)
 |    |    |    |    |-- mnc: long (nullable = true)
 |    |    |    |    |-- pdcp_loss_dl: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_lost_ho_dl: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_lost_perf_dl: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_lost_ul: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_rec_dl: long (nullable = true)
 |    |    |    |    |-- per_drb_packet_rec_ul: long (nullable = true)
 |    |    |    |    |-- pgw_s5s8_ipv4_addr: string (nullable = true)
 |    |    |    |    |-- pgw_s5s8_teid: long (nullable = true)
 |    |    |    |    |-- plani_eci: long (nullable = true)
 |    |    |    |    |-- plani_mcc: long (nullable = true)
 |    |    |    |    |-- plani_mnc: long (nullable = true)
 |    |    |    |    |-- plani_rat: string (nullable = true)
 |    |    |    |    |-- plani_tac: long (nullable = true)
 |    |    |    |    |-- reason: string (nullable = true)
 |    |    |    |    |-- report_config_type: string (nullable = true)
 |    |    |    |    |-- ringingTS: long (nullable = true)
 |    |    |    |    |-- rsrp: long (nullable = true)
 |    |    |    |    |-- rsrq: double (nullable = true)
 |    |    |    |    |-- s1_release_cause: string (nullable = true)
 |    |    |    |    |-- sgw_ipv4_addr: string (nullable = true)
 |    |    |    |    |-- sgw_teid: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_0: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_1: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_2: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_3: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_4: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_5: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_6: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pucch_7: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_0: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_1: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_2: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_3: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_4: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_5: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_6: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_7: long (nullable = true)
 |    |    |    |    |-- sinr_meas_pusch_8: long (nullable = true)
 |    |    |    |    |-- sourceAddressV4: string (nullable = true)
 |    |    |    |    |-- stream_type: string (nullable = true)
 |    |    |    |    |-- success: long (nullable = true)
 |    |    |    |    |-- successfulProcedure: boolean (nullable = true)
 |    |    |    |    |-- t3gpp_cause: long (nullable = true)
 |    |    |    |    |-- t_last_cqi: long (nullable = true)
 |    |    |    |    |-- t_last_meas: long (nullable = true)
 |    |    |    |    |-- tac: long (nullable = true)
 |    |    |    |    |-- tbs_pwr_restricted: long (nullable = true)
 |    |    |    |    |-- tbs_pwr_unrestricted: long (nullable = true)
 |    |    |    |    |-- term_addr6: string (nullable = true)
 |    |    |    |    |-- term_port: long (nullable = true)
 |    |    |    |    |-- toAudioPort: long (nullable = true)
 |    |    |    |    |-- toImsi: long (nullable = true)
 |    |    |    |    |-- toMsisdn: long (nullable = true)
 |    |    |    |    |-- to_user_agent: string (nullable = true)
 |    |    |    |    |-- up: struct (nullable = true)
 |    |    |    |    |    |-- bad_frames: long (nullable = true)
 |    |    |    |    |    |-- codec_name: string (nullable = true)
 |    |    |    |    |    |-- codec_rate: double (nullable = true)
 |    |    |    |    |    |-- lost_frames: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_jumpback: long (nullable = true)
 |    |    |    |    |    |-- max_sequence_out_of_order: long (nullable = true)
 |    |    |    |    |    |-- noise_packets: long (nullable = true)
 |    |    |    |    |    |-- packets: long (nullable = true)
 |    |    |    |    |    |-- payload_bytes: long (nullable = true)
 |    |    |    |    |    |-- sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |    |-- sequence_restarts: long (nullable = true)
 |    |    |    |    |    |-- speech_packets: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_jumpbacks: long (nullable = true)
 |    |    |    |    |    |-- sum_of_sequence_out_of_orders: long (nullable = true)
 |    |    |    |    |-- user_ipv6_addr: string (nullable = true)
 |    |    |    |    |-- validProcedure: boolean (nullable = true)
 |    |    |    |    |-- voice_integrity_dl: double (nullable = true)
 |    |    |    |    |-- wlanNodeId: long (nullable = true)
 |    |    |    |-- header: struct (nullable = true)
 |    |    |    |    |-- highlighted: array (nullable = true)
 |    |    |    |    |    |-- element: long (containsNull = true)
 |    |    |    |    |-- loc_id: string (nullable = true)
 |    |    |    |    |-- rat: string (nullable = true)
 |    |    |    |    |-- ts: long (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |-- location_history: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- cellBeamDirection: double (nullable = true)
 |    |    |    |-- cellId: string (nullable = true)
 |    |    |    |-- cellLat: double (nullable = true)
 |    |    |    |-- cellLon: double (nullable = true)
 |    |    |    |-- cellName: string (nullable = true)
 |    |    |    |-- cellTechnology: string (nullable = true)
 |    |    |    |-- ts: long (nullable = true)
 |-- ran_slice_kpis: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- dir: string (nullable = true)
 |    |    |-- levels: array (nullable = true)
 |    |    |    |-- element: long (containsNull = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- offsets: array (nullable = true)
 |    |    |    |-- element: long (containsNull = true)
 |    |    |-- values: array (nullable = true)
 |    |    |    |-- element: double (containsNull = true)
 |-- slice_kpis: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- dir: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- values: array (nullable = true)
 |    |    |    |-- element: long (containsNull = true)
```

#### Calculating count of json data 
`jsonDF.count`
O/P 
res10: Long = 13

#### Query dimensions msidn for mo and mt
`jsonDF.select($"dimensions.mo.msisdn",$"dimensions.mt.msisdn").show(false)`
O/P
```
+---------------+---------------+
|msisdn         |msisdn         |
+---------------+---------------+
|436656260039917|18562260825937 |
|594884152255703|537759801765950|
|310794610962089|38741188157402 |
|922945345389191|null           |
|960611600371254|642287652718123|
|436954649222008|680294279509464|
|54142200393010 |null           |
|163399292355395|93030896287034 |
|920821253071500|359405423966996|
|734332748737767|266110662829242|
|577079727995213|467993731289450|
|472418322496548|803039078307601|
|99607371589106 |183061325641478|
+---------------+---------------+
```

#### Filter null records
`jsonDF.filter("dimensions.mt.msisdn is not null").select($"dimensions.mo.msisdn",$"dimensions.mt.msisdn").show(false)`
O/P
```
+---------------+---------------+
|msisdn         |msisdn         |
+---------------+---------------+
|436656260039917|18562260825937 |
|594884152255703|537759801765950|
|310794610962089|38741188157402 |
|960611600371254|642287652718123|
|436954649222008|680294279509464|
|163399292355395|93030896287034 |
|920821253071500|359405423966996|
|734332748737767|266110662829242|
|577079727995213|467993731289450|
|472418322496548|803039078307601|
|99607371589106 |183061325641478|
+---------------+---------------+
```

#### Using same thing using SQL query
`jsonDF.registerTempTable("edcr_kpi")` \
`sqlContext.sql("SELECT dimensions.mo.msisdn AS mo_msisdn , dimensions.mt.msisdn AS mt_msisdn FROM edcr_kpi WHERE dimensions.mt.msisdn IS NOT NULL").show(false)`

#### Select Dimensions MO data using SQL query
`val dimensionsMODF = sqlContext.sql("SELECT dimensions.mo.msisdn as msisdn,dimensions.mo.imeisvn as imeisvn,dimensions.mo.imeitac as imeitac,dimensions.mo.imsi as imsi,dimensions.mo.src_addr as src_addr,dimensions.mo.terminal_type as terminal_type,dimensions.mo.uplane_gw_addr as uplane_gw_addr,dimensions.mo.user_agent as user_agent FROM edcr_kpi")`

#### Save above data in HDFS in tab format with header
`val header:String = "msisdn\timeisvn\timeitac\timsi\tsrc_addr\tterminal_type\tuplane_gw_addr\tuser_agent"
dimensionsMODF.rdd.coalesce(1).map(x=>x.mkString("\t")).mapPartitions(iter => Iterator(header) ++ iter).saveAsTextFile("/user/sparkuser/myspark/data/output/EDCR_MO_Dimensions")`

#### Finding errors in the log files and counting errors using accumulators and print the results
`val logsRDD = sc.textFile("/user/sparkuser/myspark/data/input/logs/*",4)` \
`val errorsAcc = sc.accumulator(0,"Errors Accumulator")` \
`val errorsLogRDD = logsRDD.filter(x => { val isError = x.contains("ERROR") ; if(isError) errorsAcc+=1 ; isError} )` \
//explicitly called saveAsTextFile to invoke Spark Action and compute Accumulator value
`errorsLogRDD.coalesce(1).saveAsTextFile("/user/sparkuser/myspark/data/output/error-logs")`
`println(errorsAcc.name+" = "+errorsAcc.value)`

#### Create a spark program for it and run it using below command
```
spark-submit \
--master local \
--executor-memory 512m \
--total-executor-cores 3 \
--name "Error Logs Count (Local)" \
--class myapps.ErrorLogsCount \
/home/sparkuser/shalini/simple-app_2.10-1.0.jar \
/user/sparkuser/myspark/data/input/logs/* \
/user/sparkuser/myspark/data/output/error-logs-local
```

```
spark-submit \
--master yarn-client \
--driver-memory 512m \
--executor-memory 512m \
--name "Error Logs Count(Yarn-client)" \
--class myapps.ErrorLogsCount \
/home/sparkuser/shalini/simple-app_2.10-1.0.jar \
/user/sparkuser/myspark/data/input/logs/* \
/user/sparkuser/myspark/data/output/error-logs-yarn-client
```

```
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 512m \
--executor-memory 512m \
--name "Error Logs Count(Yarn-cluster)" \
--class myapps.ErrorLogsCount \
/home/sparkuser/shalini/simple-app_2.10-1.0.jar \
/user/sparkuser/myspark/data/input/logs/* \
/user/sparkuser/myspark/data/output/error-logs-yarn-cluster
```

#### To see application logs  when running spark job  in Yarn Cluster
`yarn logs -applicationId [application_id]`


## Spark-SQL & JDBC
`val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://sandbox.technocrafty:3306","dbtable" -> "serviceorderdb.productinfo")).load()`
