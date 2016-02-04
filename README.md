# Products Recommender System improved with social netowrk information
## General information
Creator: Simone Cogno, simone.cogno@master.hes-so.ch

School: HES-SO//Master

Client and location: Saint-Imier HE-ARC Data analytics group

Professors: 
* Ghorbel Hatem, HE-ARC Data analytics group, Hatem.Ghorbel@he-arc.ch
* Punceva Magdalena, HE-ARC Data analytics group, magdalena.punceva@he-arc.ch

Expert: David Jacot, Swisscom SA, david.jacot@gmail.com

## Folders structure
```
.
├── Documentation (Rapport, specification, minutes)
├── Sources (All useful code source)
│   ├── GoodreadsPythonAPI (Python API)
│   ├── HybridRS (Scala code for the Hybrid RS )
│   ├── TrustWalkers (Scala code of the Trust Walkers algorithm )
│   └── webui (Web App. for views the results)
└──README.md
```


# Installation guide
## Goodreads Python API
To use the Python API used to download the Goodreads datset you need the following python libraries:
```
sudo pip install cassandra-driver --upgrade
sudo pip install coloredlogs 
sudo pip install oauth2 
pip install requests
sudo pip install beautifulsoup4
sudo pip install -U textblob
sudo python -m textblob.download_corpora #sentiment analysis tool
```
If you have some error installing the cassandra-driver you can try to install the folowing libraries:
```
sudo apt-get install libffi-dev
sudo apt-get install libssl-dev
sudo apt-get install python-dev
```

You can then able to run the scripts contained in the ```GoodreadsPythonAPI```folder.
## Cassandra installation and configuration
It's possible to install Cassandra by using a ready EC2 AMI and by configuring it by the guide found in the article [Installing a Cassandra cluster on Amazon EC2]. 
By following this instruction we can launch a Cassandra cluster in three steps:
* Launching the DataStax Community AMI
* Creating an EC2 security group
* Creating a key pair
* Connecting to your DataStax Community EC2 instance
* Clearing the data for an AMI restart
* Expanding a Cassandra AMI cluster

As we can see the steps for launching a Cassandra node are similar to those used to launch a normal EC2 instance from AWS.
For adding more node (point 6) we can use the OpsCenter fount at ```http://INSTANCE\_IP:8888``` and use a graphical wizard to add and launch a new node, for this part all the configuration are made transparently to the administrator.

When the cluster is ready we can open the CQL console by typing the ```cqlsh``` command on the console. We can, then, create our tables by initializing first our keyspace by the following CQL command

```
    calsh> CREATE KEYSPACE prs
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
```

The "Simple Strategy" means that we will use only one datacenter and the "replication\_factor=3" means that we have three replicas of every row placed on different nodes. With the Simple Strategy, the replicas are placed on the next node of the cluster clockwise. If we want to use multiple datacenters then, we can use the keyword "NetworkTopologyStrategy" that places the replicas on different datacenter for better fault tolerance (even if an entire region fail).

We can then the create a table by the following CQL command:
```
calsh>CREATE TABLE users (
id uuid PRIMARY KEY,
name varchar,
...
);
```
## Install Spark
Our machine have Java 1.7.x or later  installed and Scala 2.10. We recommend to use the same version to make the code build and run without problems.
Spark can be downloaded from the [official spark] page and it's sufficient to unpack the zip file in a folder of your choice.
Then, set the environment environment variable to your path:
```
export PATH = $PATH:YOURFOLDER/spark/bin
```

Within the folder you can run a Spark application using the ```./spark-submit``` or using the console by run the ```./spark-shell``` program.
You can also follow this [tutorial].

To configure a Spark cluster and execute an application in the cluster refer to the thesis document.

## Spark cluster set-up
To execute a Spark application on a EC2 Cluster we need to do the following steps:
* Launch a Spark EC2 cluster
* Connect to the Master driver
* Build the source code in local and create a jar containing all the necessary dependencies
* Copy the compiled JAR  to the Master of the EC2 cluster
* Configure the credentials on the Master to connecting on AWS S3 
* Run the spark application
* Monitor the running application on the Spark UI




### Launch a Spark EC2 cluster

To launching an  EC2 Cluster we can use the  spark-ec2 script contained in the Spark folder ```$SPARK\_HOME/spark1.5.1/ec2/```. From this folder, we can run the following command.
```
./spark-ec2 \
--key-pair=PUBLIC_KEY_NAME \
--identity-file=PUBLIC_KEY_NAME.pem \
--region=eu-west-1 \
--zone=eu-west-1a  \
--slaves=6 \
--instance-type=m4.xlarge \
launch \
NAME_CLUSTER
```
In the code below we have launched a EC2 cluster in Ireland in the availability zone Eu-west-1a. We can see that we have started a total of 6 machines where 1 is the Master driver and the remaining 5 are the Slaves.
For our algorithm, we typically use the instance type m4.xlarge that has an equilibrate performance in the computation as well as in memory. This instance has, in fact, a computational performance of 13 ECU  and 16 Gib of RAM memory. At this time, it cost $0.239 pro hours.
We can then calculate the approximative cost of a job of 2 hours with this configuration we have to multiplicative this cost for every machine for every hour of computation.

### Connect to the Master driver
When the EC2 Cluster is started successfully we can connect with SSH on it with the following command:
```
SPARK_HOME/spark1.5.1/ec2/spark-ec2 \
--key-pair=PUBLIC_KEY_NAME \
--identity-file=PUBLIC_KEY_NAME.pem \
--region=eu-west-1 \
--zone=eu-west-1a login NAME_CLUSTER
```

### Build and include the dependencies
To executing the application in the Cluster, we have to provide the Master all the necessary dependencies. To doing this, it is recommended to use the SBT compiler for Scala and the plugin [assembly]. This plugin allows creating a big Jar containing all the dependencies described in the build.sbt file in the project directory.

In out build.sbt file we have listen the following dependencies:

```
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1" %  "provided"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2" % "provided"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.9"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.0-M2"
```
The Spark-core library is the basic library that provides the basic functionality of spark. Then the ```Spark-SQL``` library allows us to communicate to the Cassandra database and perform some relational operations on it. The ```spark-mllib``` and the ```spark-graphx``` are libraries that provides some useful functions to make easier to develop a machine learning application and when we have to manipulate a network structure respectively.
The ```cassandra-driver-core``` is the driver that allows us to read and write data from Cassandra using the Spark.Cassandra Connector listed Abbot in the build file.
To starting the building of the Jar file use the following command:
```
sbt clean assembly
```
This generate a jar in the ```\$PROJECT\_DIR/target/scala-2.10/``` directory. 

We can run then a Spark spplication in local, to testing propose by running the following command:
```
.spark-submit \
--class main.scala.HybridRecomendationWithEvaluationOptimized \
--master local[8] target/scala-2.10/my-project-assembly.jar 2>&1 | tee log.txt
```



### Setting up the Master
When we have a running EC2 Cluster we can upload our compiled jar with all the dependencies on the Master for example via SSH. 

To accessing to the the S3 service we have to export the AWS credentials in the environment variables .
```
export AWS_ACCESS_KEY_ID=XXXXXXXXX
export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXX
```


### Run a Spark application
Then we can run a Spark application by using the ```./spark-submit``` script as showed below:
```
./spark-submit \
--class main.scala.HybridRecomendation \
--master spark://MASTER-IP:7077 \
my-project-assembly.jar 2>&1 | tee log.txt
```


Created by Simone Cogno, simone.cogno@master.hes-so.ch

[tutorial]:http://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm
[official spark]:http://spark.apache.org
[assembly]:https://github.com/sbt/sbt-assembly
[Installing a Cassandra cluster on Amazon EC2]:http://docs.datastax.com/en/cassandra/2.1/cassandra/install/installAMI.html