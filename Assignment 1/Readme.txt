Readme-

Unzip the folder by double clicking on it

--Running WordCount on Standalone mode
	1. Create Maven project in IDE
	2. Give Group_ID as cs6240 and Artifact_ID as wc
	3. Copy and paste Joe's pom.xml
	4. Copy config folder parallel to src
	5. The src/main/java should contain WordCount.java
	6. Create input directory parallel to src containing the 1.3 GB input file
	6. The pom, makefile, src, input and config should be in the same directory

--Following changes are to be made to makefile
	1. Change location of hadoop to location on your system (type whereis hadoop)
	2. Change hdfs user name to name of your local system

--Run the following commands
	1. Make switch-standalone
	2. Make alone

Post execution check output folder -
	1. It contains .class of WordCount and 'part' files which depend on reducer tasks currently 1.  
	2. Output folder is deleted after every execution

--Running WordCount on EMR
	1. Installing AWS CLI
		i. Download the access key and password from AWS web console
		ii. Launch EC2  
		iii. Edit security groups
		iv. Install SSH client
		v. Install PIP- sudo pip install awscli --ignore-installed-six
		vi. Check if correctly installed through aws help.
		vii. Configure aws using -- aws configure with access key and password
		viii. Create default roles through -- emr create-default-roles
		ix. Changes in Makefile : change bucket name ; aws instance type to m1.medium; aws subnet id (according to your region) ; nodes : 2 ; 
		x. Add the following to pom.xml
			<plugin>
			     <groupId>org.apache.maven.plugins</groupId>
     				<artifactId>maven-jar-plugin</artifactId>
				     <version>2.4</version>
				<configuration>
				     <archive>
					<manifest>
     					<mainClass>org.apache.hadoop.examples.WordCount</mainClass>         
					</manifest>
				    </archive>
				</configuration>
			</plugin>
		xi. Create input folder which contains file present in assignment (http://www.ccs.neu.edu/course/cs6240f14/)
		xii. Upload input -- upload-input-aws (Please run commands through the folder structure you create for your AWS)
		xiii. Run make cloud to launch your cluster 
		xiv. Once it runs, check syslog
		xv. S3 contains output(3 files) files download these using -- make download-output-aws

-- Running Multi threading programs
	1. ClimateDataMain 	- Main file which needs to be run throuogh IDE
				- Any changes in input file source can be done here in variable = csvFile
				- cores can be increased here as well to check failures in variable = noOfThreads
				- Currently my program has Fibonacci, please comment it if necessary.
	2. AccumulationDataStructure
	3. ClimateData_Sequential
	3. ClimateData_CourseLock	CourseLockAverage
	4. ClimateData_FineLock		FineLockAverage
	5. ClimateData_NoLock		NoLockAverage
	6. ClimateData_NoShared		NoSharedAverage


 

 


