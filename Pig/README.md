# Pig Quick commands

### How to start pig grunt shell
`pig -x mapreduce`

### Run a script in local mode
Create a .pig file \
`vi /home/cloudera/shalini/pig/scripts/myscript.pig`

Add below statements and save it
```
/*myscript.pig
My script is simple.
It includes three Pig latin statements
*/
A = load '/user/shalini/datasets/empsalary.txt' using PigStorage() as (empid:int,name:chararray,age:int,salary:int,gender:chararray);--loading data
B = foreach A generate name; --transforming data
DUMP B;--retrieving results
```
Go back to grunt shell and run the script using run command . When pig is running in local mode, use local path of the script else HDFS for cluster mode \
`run /home/cloudera/shalini/pig/scripts/myscript.pig`

### Parameter substitution
Add following statements  in script 

```
/*myscript.pig
My script is simple.
It includes three Pig latin statements
*/
SET pig.logfile /home/cloudera/shalini/logs/piglog.log
SET debug on
SET job.name 'Emp names'
--SET default_parallel 2 -- 2 reducers will be used
emp_records  = load '/user/shalini/datasets/empsalary.txt' using PigStorage() as (empid:int,name:chararray,age:int,salary:int,gender:chararray);--loading data
emp_valid_records = filter emp_records by name is not null;--filtering invalid records
emp_names = foreach emp_valid_records generate name; --transforming data
STORE emp_names INTO '$output' USING PigStorage();--retrieving results
```
Run the above script as \
`run -param output=/user/shalini/pig/output/  /home/cloudera/shalini/pig/scripts/myscript.pig`
or \
Can use **exec** instead of **run**. **exec** runs in batch mode and do not remember aliases in shell \
`exec -param output=/user/shalini/pig/output/  /home/cloudera/shalini/pig/scripts/myscript.pig`

It would be good to set error log at specific location through command shell
`SET pig.logfile /home/cloudera/shalini/logs/piglog.log`

### Load data from Hive table "departments" in Pig
In case, Pig do not pickup HCatalog jars run Pig as `pig -useHCatalog` \
`A = load 'default.departments' USING org.apache.hive.hcatalog.pig.HCatLoader();` (Validate Loading of hive table 'departments' in pig) \
 `dump A;` (Load table mentioned in A and It will print table content existing in departments table in grunt shell console)

`describe A;` 
```
A: {department_id: int,department_name: chararray} (Department name of type 'string' is converted to 'chararray' type)
```

### Store pig script results data in Hive table 
a) First create managed table in hive \
`create table pig_empnames (name string);`
b)Add following line for storage using **HCatStorer** in your script.
```
STORE emp_names INTO '$hive_table_name' USING org.apache.hive.hcatalog.pig.HCatStorer();
--storing results in Hive managed table using HCatStorer
```
c)Run the script passing hive_table_name as **pig_empnames** \ 
`exec -param output=/user/shalini/pig/output/ -param hive_table_name=pig_empnames /home/cloudera/shalini/pig/scripts/myscript.pig;`

d) view  the output using select query in Hive
`select * from pig_empnames;`

### For using Tez execution engine(default is mapreduce), start Pig as  
`pig -x tez`

If getting any error related to auto parallelism, add **default_parallel value** in pig script

### Pig Script Example with join. Write below script
```
/*sum_of_hours_miles
--Script to calculate the number of hours
--logged by drivers for driving miles
*/
set debug off;
set  default_parallel 1;
set pig.logfile /home/technocrafty/logs/mylogfile.log;
drivers = LOAD '/user/technocrafty/shalini/datasets/drivers.csv' USING PigStorage(',');
raw_drivers = FILTER drivers BY $0>1;
driver_details = FOREACH raw_drivers GENERATE $0 as driverId,$1 as name;

timesheet = LOAD '/user/technocrafty/shalini/datasets/timesheet.csv' USING PigStorage(',');
raw_timesheet = FILTER timesheet BY $0>1;
timesheet_logged = FOREACH raw_timesheet GENERATE $0 as driverId,$2 as hours_logged,$3 as miles_logged;
grp_logged = GROUP timesheet_logged by driverId;
sum_logged = FOREACH grp_logged GENERATE group AS driverId, 
SUM(timesheet_logged.hours_logged) AS  hours_logged,
SUM(timesheet_logged.miles_logged) AS  miles_logged ;

join_driver_data = JOIN sum_logged BY driverId, driver_details BY driverId;
driver_data = FOREACH join_driver_data GENERATE 
$0 AS driverId, 
$4 AS NAME, 
$1 as hours_logged,
$2 as miles_logged  ;

STORE driver_data INTO '/user/technocrafty/shalini/pig/output/drivers_logged_hours' USING PigStorage('\t');
```

Run as 
`exec /home/technocrafty/shalini/pig/scripts/sum_of_hours_miles.pig;`
