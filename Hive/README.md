# Hive Quick commands

### Command to create a table using CTAS 
`CREATE TABLE nested AS SELECT array(array(1, 2), array(3, 4)) FROM dummy;`

### Create a table in Hive and load data from local file system file where rows are separted by ,

`CREATE TABLE UserRecords(first_name String,last_name String,address String,country String,city String,state String,post String,phone1 String,phone2 String,email String,web String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;`

`LOAD DATA LOCAL INPATH '/home/cloudera/shalini/Datasets/UserRecords.txt' OVERWRITE INTO TABLE UserRecords;` \
It creates a file UserRecords.txt in `/user/hive/warehouse/userrecords folder` (This is hive warehouse folder in cloudera VM)


##### To load data from a file in HDFS. It moves the original file student.txt from /datasets/ to Hive warehouse (Managed table example. Drop table student will delete the metdata as well data from hive warehouse dir)
`create table student(id tinyint,name varchar(20))row format delimited fields terminated by ',';` \
`load data inpath '/user/shalini/datasets/student.txt' into table student;`

##### In order to retain the data lost in managed table, external table can be used. It wil only delete the metadata.Syntax is below where location determines the external location where table will be created
`create external table student(id tinyint,name varchar(20))
 row format delimited fields terminated by ',' 
 location '/user/shalini/hive_external_table';` \
`load data inpath '/user/shalini/datasets/student.txt' into table student;`

### Create a partition
`CREATE TABLE empsalary(empid smallint,name varchar(20),age tinyint,salary int)
PARTITIONED BY(gender varchar(6)) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';` 

##### Loading data to table default.empsalary partition (gender=Male) 
`LOAD DATA LOCAL INPATH '/home/cloudera/shalini/Datasets/empMaleSalary.txt' 
OVERWRITE INTO TABLE empsalary 
partition(gender='Male');` \

##### Loading data to table default.empsalary partition (gender=Female) 
`LOAD DATA LOCAL INPATH '/home/cloudera/shalini/Datasets/empFemaleSalary.txt' 
OVERWRITE INTO TABLE empsalary 
partition(gender='Female');`

##### View partitions on empsalary table
`show partitions empsalary;`

### Bucketing
`CREATE TABLE bucketed_empsalary(empid smallint,name varchar(20),age tinyint,salary int,gender varchar(6)) 
CLUSTERED BY (empid) INTO 2 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';` \
`LOAD DATA LOCAL INPATH '/home/cloudera/shalini/Datasets/empsalary.txt' OVERWRITE INTO TABLE bucketed_empsalary;`

`SET hive.enforce.bucketing=true;`

##### This will insert the data in form of buckets.
`INSERT OVERWRITE TABLE bucketed_empsalary select * from empsalary;`


##### It will create 2 buckets based on distinguished empid. Data from buckets can be retrieved using `TABLESAMPLE(bucket 1 out of 2);` It will return all the records in one bucket. In case of non bucketed table, on rand() would have returned any random values;
`select * from empsalary tablesample(bucket 2 out of 2 on rand());`

### Save Data in Several Formats
##### In textfile
`create table text_empsalary stored as textfile as select * from empsalary;`

##### In avro format. In order to save in avro format, set following properties
`SET hive.exec.compress.output=true;` \
`SET hive.output.codec=snappy;` \
`create table avro_empsalary stored as avro as select * from empsalary;`

##### In sequence file format
`create table seq_empsalary stored as sequencefile as select * from empsalary;`

##### In parquet format
`create table parquet_empsalary stored as parquet as select * from empsalary;`

### Multi-table data load. First create table structure then write query defining source and queries from where data need to be popoulated
`create table orders_by_customerId(order_customer_id int,order_id int);` \
`create table orders_by_status(order_status string,order_id int);` \
`create table orders_by_date(order_id int,order_date string,order_customer_id int,order_status  string);`\
`from orders 
insert overwrite table orders_by_customerId 
select order_customer_id,count(distinct order_id)
group by order_customer_id
insert overwrite table orders_by_status
select order_status,count(distinct order_id)
group by order_status
insert overwrite table orders_by_date
select * order by order_date desc;` 

### Create table..as select
`create table target as select order_id,order_status from orders;`

### Alter tables
`alter table target rename to orders_target;` \
`alter table student rename to external_table_student;` (In case of external tables, only metadata is updated) \
`alter table external_table_student add columns(age tinyint);` (Add new column in  external table student) \
`alter table copy_schem_empsalary change column salary sal int;` (change col name) \
`alter table copy_schem_empsalary change column sal sal smallint;`(change col type) \
`alter table copy_schem_empsalary change sal sal int after name;`(place sal column after name) \
`alter table copy_schem_empsalary change sal sal int first;`(place at first position) \
`alter table copy_schem_empsalary replace columns(empid smallint,name varchar(20),age tinyint,sal int);`(replace all columns with new ones) 

### How to view metadata information in hive
`show create table external_table_student;`

### Copy schema of a table and load data in new copied table
`create table copy_schem_empsalary like empsalary;` \
`create external table copy_schema_external_table like external_table_student;` \
`insert overwrite table copy_schema_external_table select * from external_table_student;`

##### Dynamic partitioning . 
##### Create normal table and populate data
`create table call_log(from_number bigint,to_number bigint,start_time string,end_time string,is_std char(1))
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';` \
`LOAD DATA LOCAL INPATH '/home/cloudera/shalini/Datasets/call_log.txt' INTO table call_log;`

##### Create structure of partition table
`create table partitioned_call_log(from_number bigint,to_number bigint,start_time string,end_time string) PARTITIONED BY(is_std char(1));`

##### Insert data  into dynamically partitioned table
`from call_log INSERT OVERWRITE TABLE partitioned_call_log partition(is_std) select from_number,to_number,start_time,end_time,is_std;`


### Views
`create view view_departments as select * from departments;`(create view);\
`alter view view_mydepartments as select * from orders;`(alters view's definition) \
`drop view view_mydepartments;`(drop view. Dropping table makes a view useless) \
`show views;`(show the views)

### Table properties
`show table extended like order_items;` \
`show tblproperties orders;`(Lists all table properties of orders table) \
`show tblproperties order_items("comment");`(Lists the value of "comment" table property in orders table) 

##### show all the columns of table)
`show columns from orders;` 

### SHOW FUNCTIONS lists all the user defined and built-in functions matching the regex. To get all functions use ".*"
`SHOW FUNCTIONS "a.*";` 

`select current_database();`

`describe extended orders;`(details about orders table) \
`describe formatted orders;`(details about orders table in formatted form) \
`describe formatted empsalary;`(To check the managed table or not); 


#####  Copy data to a local directory
`insert overwrite local directory '/home/cloudera/shalini/hive' row format delimited fields terminated by '\t' stored as textfile select * from orders;`

##### Multiple inserts into local directory, hdfs location & table using single table and command
`from orders
insert overwrite local directory '/home/cloudera/shalini/hive/' row format delimited fields terminated by '\t' stored as textfile
select#####  * 
insert overwrite directory '/user/shalini/hive/orders/' row format delimited fields terminated by '\t' stored as textfile
select * 
insert into table temp_orders
select * `

`insert into table text_empsalary values(1214,'Shalini',32,50000,'Female');`(Insert single row)

`CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3, 2))
CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');` 

`INSERT INTO TABLE students VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32),('shalini',32,5.1);` 

`CREATE TABLE pageviews(userid VARCHAR(64), link STRING, came_from STRING) 
PARTITIONED BY (datestamp STRING) CLUSTERED BY (userid) INTO 256 BUCKETS STORED AS ORC;`

`INSERT INTO TABLE pageviews PARTITION (datestamp = '2014-09-23') VALUES ('jsmith', 'mail.com', 'sports.com'), ('jdoe', 'mail.com', null);`

`select * from pageviews where datestamp ='2014-09-23';` (To see partitioning data) 

##### To enable dynamic partitioning
`set hive.exec.dynamic.partition.mode=nonstrict`

`INSERT INTO TABLE pageviews PARTITION (datestamp) VALUES ('tjohnson', 'sports.com', 'finance.com', '2014-09-23'), ('tlee', 'finance.com',null, '2014-09-21');`
 

### ACID Transactions
##### Pre-requisite : In order to do ACID transactions[UPDATE DELETE & MERGE], following properties must be added in hive-site.xml

```<property>
  <name>hive.support.concurrency</name>
  <value>true</value>
 </property>
 <property>
  <name>hive.enforce.bucketing</name>
  <value>true</value>
 </property>
 <property>
  <name>hive.exec.dynamic.partition.mode</name>
  <value>nonstrict</value>
 </property>
 <property>
  <name>hive.txn.manager</name>
  <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
 </property>
 <property>
  <name>hive.compactor.initiator.on</name>
  <value>true</value>
 </property>
 <property>
  <name>hive.compactor.worker.threads</name>
  <value>2</value>
 </property>
```
Restart hive server as `sudo service hive-server2 restart`

### Table should be ORC and bucketed and have transactional=true for ACID transactions
`CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3, 2))
CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES('transactional'='true');` 

`INSERT INTO TABLE students VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32),('shalini',32,5.1);`

`delete from students where name='shalini';` \
`update students set name='Shalini Goel' where name='shalini';`


### Compressed Store. Store the file as Sequence format for better utillization of hadoop cluster
`SET hive.exec.compress.output=true;` \
`SET io.seqfile.compression.type=BLOCK;` 


`CREATE TABLE raw (line STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';`
`LOAD DATA LOCAL INPATH '/home/cloudera/Datasets/bbc.tar.gz' INTO TABLE raw;`
`CREATE TABLE raw_sequence (line STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS SEQUENCEFILE;`
`INSERT OVERWRITE TABLE raw_sequence SELECT * FROM raw;`

### Subqueries
`select customer_name from (select concat(customer_fname,' ',customer_lname) as customer_name from customers) outer_query limit 5;
union all
select name from (select concat(customer_fname,' ',customer_lname) as name from customers union all select name from students) outer_query where name like 'S%';`
