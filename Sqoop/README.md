# Sqoop Quick commands

Using password from a file
1. Create a sqoop.password file in local FS(Use  -n in echo command so as to prevent a newline in it). Set 400 permission 
echo -n "cloudera" > /home/cloudera/sqoop.password
chmod 400 /home/cloudera/sqoop.password
2. Upload it in home directory of HDFS
hdfs dfs -put /home/cloudera/sqoop.password /user/shalini
3. Use password file in sqoop commands like below
sqoop list-databases --connect jdbc:mysql://quickstart.cloudera --username root --password-file /user/shalini/sqoop.password

4. to view all tables in specifc DB named as retail_db
sqoop list-tables --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password

5. To pass passowrd from console (It will prompt for a password to enter)
sqoop list-databases --connect jdbc:mysql://quickstart.cloudera --username root -P

6.Import all tables from a database at once in a warehouse folder in HDFS
sqoop import-all-tables -connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --warehouse-dir /user/shalini/sqoop/warehouse -m 1 --fields-terminated-by '\t'

7. Import specific columns as text file using 1 mapper, where clause, delete target directory
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --table customers --target-dir /user/shalini/sqoop/customers_table --as-textfile --columns "customer_id,customer_fname,customer_lname" --delete-target-dir --where "customer_id<=10" -m 1

Other file formats :
--as-avrodatafile	Imports data to Avro Data Files
--as-sequencefile	Imports data to SequenceFiles
--as-textfile	Imports data as plain text (default)
--as-parquetfile	Imports data to Parquet Files

8. Free form join query (Remove --tables and add --query and --where) Running as single mapper
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --target-dir /user/shalini/sqoop/customers_table --as-textfile --query "select c.category_name,d.department_name from categories c , departments d where \$CONDITIONS" --delete-target-dir --where "c.category_department_id=d.department_id" -m 1

Also can use --autoreset-to-one-mapper instead of -m 1 when table has no primary key

9. Free form join query running as multiple mappers with split-by . Removed -m 1
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --target-dir /user/shalini/sqoop/customers_table --as-textfile --query "select c.category_id,c.category_name,d.department_name from categories c , departments d where \$CONDITIONS" --delete-target-dir --where "c.category_department_id=d.department_id" --split-by "c.category_id"

10. Replaced  target-dir with warehouse-dir so that default table name is used as destination directory. Note. Table name is must in order to use warehouse-dir
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --table customers --warehouse-dir /user/shalini/sqoop/ --as-textfile --columns "customer_id,customer_fname,customer_lname"  --where "customer_id<=10" -m 1 --delete-target-dir

11. Incremental import with append. It appends the file with only last inserted values.
sqoop import --incremental append --check-column "order_id" --last-value 68883 --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --table orders --target-dir /user/shalini/sqoop/orders_table --as-textfile -m 1

12. Incremental import with last modified. It appends the file with updated rows. Note: to use --append when using incremental last modified and target dir already exists.
sqoop import --append --incremental lastmodified --check-column "order_date" --last-value "2014-07-24 00:00:01" --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --table orders --target-dir /user/shalini/sqoop/orders_table --as-textfile -m 1

13. Use of --null-string. It replace null value in table with "\N" also --append do not need --delete-target-dir. It creates new file.
sqoop import --append --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --table departments --target-dir /user/shalini/sqoop/departments_table --as-textfile --null-string '\\N' -m 1

14. Using delimiters
sqoop import --append --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --table products --target-dir /user/shalini/sqoop/products_table --as-textfile --fields-terminated-by '\t' --lines-terminated-by '\n' -m 1

Sqoop & Hive
1. In hive, a new empty table is created with same definition. Sqoop imports departments table from mysql and create a table in hive with same definitions.
sqoop --create-hive-table --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-table departments --table departments

2. Import data into hive using sqoop
If table already exists (do not use --create-hive-table )
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-import --hive-table departments --table departments --target-dir /user/shalini/sqoop/departments_table --delete-target-dir -m 1


If table already do not exist (add--create-hive-table )
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-import --create-hive-table --hive-table departments --table departments --target-dir /user/shalini/sqoop/departments_table --delete-target-dir -m 1

or
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-import --hive-table customers --table customers --create-hive-table -m 1

Example: Hive table overwrite when sqoop import (Both hive-import an hive-overwrite)
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-import --hive-overwrite --hive-table departments --table departments --null-string 'XXX'

Below command will create file in HDFS at /user/hive/warehouse/departments folder (How?)
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-import --hive-overwrite --hive-table departments --table departments  --null-string 'AAA' -m 1

Below command do not use  hive-import and it will create file in HDFS at specified location
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-overwrite --hive-table departments --table departments --target-dir /user/shalini/sqoop/departments --null-string 'YYY' -m 1

-------------------------------------------------------------------
Qn: Below command will create file in HDFS at /user/hive/warehouse/departments folder (How?)sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --hive-import --hive-overwrite --hive-table departments --table departments  --null-string 'AAA' -m 1
Qn: What happens when both hive-import and hive-overwrite are used?
Qn: difference between hive-import & hive-overwrite
Qn: Is there any command to import the data to both HDFS and hive parallely?

Sqoop Export
Import a join table results in HDFS for export later
sqoop import --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --target-dir /user/shalini/sqoop/category_by_dept --as-textfile --query "select c.category_name,d.department_name from categories c , departments d where \$CONDITIONS" --delete-target-dir --where "c.category_department_id=d.department_id" -m 1

Export data 
sqoop export --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --export-dir /user/shalini/sqoop/category_by_dept --table category_by_dept

Specifying columns and update mode  will export only specific columns and if column already exists, but will write all rows again and again until update-key is added 
sqoop export --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --export-dir /user/shalini/sqoop/category_by_dept --table category_by_dept --columns "category_name,department_name" --update-mode updateonly

codegen - It will generate code at location specified by --bindir and java file is created in outdir
sudo sqoop codegen --connect jdbc:mysql://quickstart.cloudera/retail_db --username root --password-file /user/shalini/sqoop.password --table customers --bindir /home/cloudera/shalini/codegen --class-name Customer --outdir src/.
