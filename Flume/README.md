# Flume Quick learning

### Install telnet server to test
`yum install telnet-server telnet`
or 
`apt-get install telenetd`

`chkconfig telnet on` \
Edit **/etc/xinetd.d/telnet** and set **disable = yes** and restart telnet
`/etc/init.d/xinetd restart`

### To start a Flume agent

```
sudo flume-ng agent 
--name agent 
--conf /etc/alternatives/flume-ng-conf/ 
--conf-file /etc/alternatives/flume-ng-conf/flume-conf.properties.template 
Dflume.root.logger=INFO,console
```

### Running a telnet Flume agent
1. Create a conf properties file 
 `echo -n "telnet-agent" > /home/cloudera/shalini/flume-ng-conf/telnet-flume-conf.properties` 
2.  `vi /home/cloudera/shalini/flume-ng-conf/telnet-flume-conf.properties` and enter following content in the file

```
#Name of the components on this agent 
telnet-agent.sources=source1
telnet-agent.sinks=sink1
telnet-agent.channels=channel1

# Describe sources (Source : Netcat listens on a port and converts each line of text into an event.)
telnet-agent.sources.source1.type=netcat
telnet-agent.sources.source1.bind=localhost
telnet-agent.sources.source1.port=44444

# Describe sinks
telnet-agent.sinks.sink1.type=logger

# Describe channels
telnet-agent.channels.channel1.type=memory
telnet-agent.channels.channel1.capacity=1000
telnet-agent.channels.channel1.transactionCapacity=100

#Binding source, sinks & channels
telnet-agent.sources.source1.channels=channel1
telnet-agent.sinks.sink1.channel=channel1
```

3. Now run following command to start the flume agent

```
sudo flume-ng agent 
--name telnet-agent 
--conf flume-ng-conf 
--conf-file /home/cloudera/shalini/flume-ng-conf/telnet-flume-conf.properties 
Dflume.root.logger=INFO,console
```
4. In order to log the events in a file instead of console. Use --conf {conf-dir} . It is directory that contains **flume-env.sh** shell script  for logging in a file instead of console
```
 sudo flume-ng agent 
--name telnet-agent 
--conf /etc/alternatives/flume-ng-conf 
--conf-file /home/cloudera/shalini/flume-ng-conf/telnet-flume-conf.properties 
Dflume.root.logger=INFO,console
 ```
5. Fan out or multiplexing (Conf file) There will be multiple channels and sinks
```
#Name of the components on this agent
telnet-agent.sources=source1
telnet-agent.sinks=sink1 sink2
telnet-agent.channels=channel1 channel2

#Describe sources
telnet-agent.sources.source1.type=netcat
telnet-agent.sources.source1.bind=localhost
telnet-agent.sources.source1.port=44444

#Describe sinks
telnet-agent.sinks.sink1.type=logger
telnet-agent.sinks.sink2.type=hdfs
telnet-agent.sinks.sink2.hdfs.path=/user/shalini/flume
telnet-agent.sinks.sink2.hdfs.filePrefix=events
telnet-agent.sinks.sink2.hdfs.inUsePrefix=_
telnet-agent.sinks.sink2.hdfs.fileSuffix=.log
telnet-agent.sinks.sink2.hdfs.fileType=DataStream

#Describe channels
telnet-agent.channels.channel1.type=memory
telnet-agent.channels.channel1.capacity=1000
telnet-agent.channels.channel1.transactionCapacity=100
telnet-agent.channels.channel2.type=file

#Binding sourcs,sinks& channels
telnet-agent.sources.source1.channels=channel1 channel2
telnet-agent.sinks.sink1.channel=channel1
telnet-agent.sinks.sink2.channel=channel2
```
6. Two tier example - Avro sink is used to join one sink to another source \
a) Write config properties file as
```
# First Agent agent 1
agent1.sources=source1
agent1.sinks=sink1
agent1.channels=channel1

#Describe source of Tier 1
agent1.sources.source1.type=netcat
agent1.sources.source1.bind=localhost
agent1.sources.source1.port=44444

#Describe sink of Tier 1
agent1.sinks.sink1.type=avro
agent1.sinks.sink1.hostname=localhost
agent1.sinks.sink1.port=55555

#Describe channels
agent1.channels.channel1.type=memory
#agent1.channels.channel1.checkpointDir=/home/cloudera/shalini/agent1/file-channel/checkpoint
#agent1.channels.channel1.dataDirs=/home/cloudera/shalini/agent1/file-channel/data

#Binding agent1 components
agent1.sources.source1.channels=channel1
agent1.sinks.sink1.channel=channel1

# Second Agent agent 2
agent2.sources=source2
agent2.sinks=sink2
agent2.channels=channel2

#Describe source of Tier 2. It should match with sink in tier1 agent
agent2.sources.source2.type=avro
agent2.sources.source2.bind=localhost
agent2.sources.source2.port=55555

#Describe sink of Tier 2. Let it be console
agent2.sinks.sink2.type=logger

#Describe channels
agent2.channels.channel2.type=memory
#agent2.channels.channel2.checkpointDir=/home/cloudera/shalini/agent2/file-channel/checkpoint
#agent2.channels.channel2.dataDirs=/home/cloudera/shalini/agent2/file-channel/data

#Binding agent1 components
agent2.sources.source2.channels=channel2
agent2.sinks.sink2.channel=channel2
```
b) start agent1 as \
`sudo flume-ng  agent --name agent1 --conf flume-ng-conf --conf-file /home/cloudera/shalini/flume-ng-conf/two-tier-flume-conf.properties -Dflume.root.logger=INFO,console` \
c) In the same way start agent2 \
`sudo flume-ng  agent --name agent2 --conf flume-ng-conf --conf-file /home/cloudera/shalini/flume-ng-conf/two-tier-flume-conf.properties -Dflume.root.logger=INFO,console`
