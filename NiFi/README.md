# Apache Ni-Fi

## NiFi Pipeline to load incoming files into cassandra database
A NiFi pipeline will be created to ingest data from source SFTP server to Common DB tables. Common DB tables are those tables which store incoming data coming from client machines. Incoming data is basically called as raw data. It has already gone through the process of cleaning, filtering & parsing at client machines. It is mostly in the form of csv or excel files. 
A Nifi pipeline is created which polls source SFTP server for these incoming files every 30 seconds, splits them into batches according to Cassandra DB batch size limit(65535 rows) , maps incoming datatypes into cassandra table datatypes and load them in respective Cassandra common DB tables.
For e.g. As soon as Input file named as out_20190323.csv is copied in SFTP server, Nifi pipeline will pick up that file every 30 seconds and maps the csv datatypes as per Common DB datatypes, split the file into batches and load it into Cassandra Common table “common.pm_huawei_4g”

![NiFi Pipeline](https://github.com/coderepairer/bigdata/blob/master/NiFi/images/nifi.png?raw=true)

### Components in above pipeline:
**1.	Get SFTP :** This component fetches the data from MBNL SFTP server(195.171.11.20) from location /ftp/xxxx/prod/pm_huawei_4g
It polls out*.csv files every 30 seconds. As soon as it ingests the file, files get deleted from SFTP server. Now the file becomes flow file in above Nif-pipeline and transfers to next component.
**2.	ReplaceText:** This component replaces the counter_ids in header of input file with counter_names as per a counter mapping sheet.
**3.	SplitText:** This component splits the incoming file into batches of 2000 records before loading it into Cassandra. 
**4.	PutCassandraRecord:** This component reads the incoming file using CSVReader. CSVReader reads the incoming file according to predefined avro/json schema. Same avro/json schema is also used to map the plain text csv data to  cassandra db column datatypes at the time of loading data into table.
Component Properties:
![Component Properties](https://github.com/coderepairer/bigdata/blob/master/NiFi/images/dbproperties.png?raw=true)
**5.	Put SFTP :** This component copies the files in failed scenarios back to SFTP server(195.171.11.20) at location /ftp/xxxx/prod/pm_huawei_4g_failed

### Variables used :
![Component Properties](https://github.com/coderepairer/bigdata/blob/master/NiFi/images/dbproperties.png?raw=true)