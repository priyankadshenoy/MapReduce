ReadME

PageRankScala folder contains src code with 2 files 1 .java 1.scala

output folder contains java and scala output which contain standalone and EMR outputs

stderr folder contains the stderr files for the two runs

Makefile and a pom.xml present in PageRankScala is used to run the code on standalone mode and on AWS

EMR Output contains output files for report ie Scala implementation

	  STANDALONE MODE
---------------------------------------------	// referenced pom.xml and makefile provided by Dhaval on discussion board
- Create a Maven Project in IDE

- Give group and artifact ID

- Add dependencies into pom.xml

- Place all java files in src/main/java

- Create input folder with
	- wikipedia-simple-html.bz2

- Copy config folder from where you installed hadoop and place it under your main project directory

- config, input pom.xml, Makefile, src, should be in the same project directory

- Make file changes
	spark.root = spark location on your local system
	jar.name = jar file that will be created
	jar.path = target/${jar.name}
	job.name = change it to the path of your main file
	Updated alone command (used spark-submit) : ${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${jar.path} ${local.input} ${local.output}

- Run the commands interminal of IDE
	- make switch-standalone
	- make alone
	- Output contains final top 100 page ranks

	
			AWS EMR
-----------------------------------------------
- Log into AWS
- Add to pom.xml
    <plugin>
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>3.2.2</version>
            <executions>
                <execution>
                    <id>scala-compile-first</id>
                    <phase>process-resources</phase>
                    <goals>
                        <goal>add-source</goal>
                        <goal>compile</goal>
                    </goals>
                </execution>
            	<execution>
                    <id>scala-test-compile</id>
                    <phase>process-test-resources</phase>
                    <goals>
                        <goal>testCompile</goal>
                    </goals>
                </execution>
            </executions>
   </plugin>
	 <plugin>
	      <artifactId>maven-compiler-plugin</artifactId>
              <version>3.3</version>
              <configuration>
                  <source>1.8</source>
                  <target>1.8</target>
              </configuration>
         </plugin>
         <plugin>
              <artifactId>maven-assembly-plugin</artifactId>
              <version>2.4</version>
              <configuration>
                  <descriptorRefs>
     	                <descriptorRef>jar-with-dependencies</descriptorRef>
                  </descriptorRefs>
              </configuration>
              <executions>
                  <execution>
                      <id>make-assembly</id>
                      <phase>package</phase>
                      <goals>
                          <goal>single</goal>
                      </goals>
                  </execution>
              </executions>      
        </plugin>
        </plugins>
    </build>
    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>2.11.8</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>2.1.0</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>3.8.1</version>
            <scope>test</scope>
        </dependency>

    </dependencies>

	
	
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

Updated the cloud command (applicationsName, steps and added configurations option):

--applications Name=Spark \
--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
--log-uri s3://${aws.bucket.name}/${aws.log.dir} \

	
- Navigate to current java project directory on terminal
- Upload data input (4 2006 wiki files) to AWS using -
	make upload-input-aws
- Run make cloud
- S3 contains Output and Log folders


			INTELLIJ
--------------------------------------------------
-Go to Run->Edit Configurations->Applications
	Click on the configuration tab 
		 Enter main class path
		 arguments : input output
		 

		FILE NAMES
---------------------------------------------------

- PageRankScala- Object that runs scala code
- PageRankPreProcessing - Job responsible for pre-processing data. Run bz2 file through this job.

