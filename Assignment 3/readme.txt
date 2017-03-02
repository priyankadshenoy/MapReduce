ReadME

PageRankSourceCode folder contains src code with 8 .java files

output folder contains 2 part-r files for output of output1-5nodes and output2-5nodes 

syslog folder contains the syslog files for the two runs

Makefile and a pom.xml present in output is used to run the code on standalone mode and on AWS

	  STANDALONE MODE
---------------------------------------------
- Create a Maven Project in IDE

- Give group and artifact ID

- Add dependencies into pom.xml

- Place all java files in src/main/java

- Create input folder with
	- wikipedia-simple-html.bz2

- Copy config folder from where you installed hadoop and place it under your main project directory

- config, input pom.xml, Makefile, src, should be in the same project directory

- Run the commands interminal of IDE
	- make switch-standalone
	- make alone

	- There will be several output folders created.output0-output10 contain preprocessing and page rank job. output contains final top 100 page ranks. outputdelta contains output from map after 10th iteration

	
			AWS EMR
-----------------------------------------------
- Log into AWS
- Add to pom.xml
	<plugin>
   		<groupId>org.apache.maven.plugins</groupId>
      		<artifactId>maven-jar-plugin</artifactId>
         	<version>2.4</version>
           	<configuration>
              	<archive>
                <manifest>
                	<mainClass>{name_of_main_class}</mainClass>         
                </manifest>
              	</archive>
           	</configuration>
	</plugin>
	
	
In the Makefile for AWS execution change
aws.region = to the region of  aws cluster
aws.bucket.name = bucket_name  for S3
aws.subnet.id = subnet_id of your region from VPC subnet list
aws.input = name of the input folder on S3
aws.output = name of the output folder on S3
aws.log.dir = name of the log folder
aws.num.nodes =  number of worker machines
aws.instance.type = type of the machine to use.

For first run  aws.num.nodes=5 and aws.instance.type=m4.large
For second run  aws.num.nodes=10 and aws.instance.type=m4.large

	
- Navigate to current java project directory on terminal
- Upload data input (4 2006 wiki files) to AWS using -
	make upload-input-aws
- Run make cloud
- S3 contains Output and Log folders


			INTELLIJ
--------------------------------------------------
- Add the following to src/main/resources
log4j.properties
	hadoop.root.logger=DEBUG, console
	log4j.rootLogger = DEBUG, console
	log4j.appender.console=org.apache.log4j.ConsoleAppender
	log4j.appender.console.target=System.out
	log4j.appender.console.layout=org.apache.log4j.PatternLayout
	log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n	

-Go to Run->Edit Configurations->Applications
	Click on the configuration tab 
		 Enter main class path
		 arguments : input output
		 

		FILE NAMES
---------------------------------------------------

- DriverProgram- Creates and runs all jobs
- PageData- Class which holds data about pages
- PageRankPreProcessing - Job responsible for pre-processing data. Run bz2 file through this job.
- PageRankJob - The page ranks for 10 iterations are calculated.
- PageRankDelta -  Extra map task for correcting delta values after 10th iteration
- PageRankTopK -  Class calculates the top 100 page ranks
- Preprocessing-  Decompresses bz2 file and parses Wikipages on each line
- HumanReadableForm - Class which parses the file into human readable format. Commented since not in use
