The following steps are to be followed for running programs for assignment 2

I have created 4 Maven Projects which has lead to creation of 4 Makefiles. Please use this for individual run of projects.

	  STANDALONE MODE
---------------------------------------------
- Create a Maven Project in IDE
- Give group and artifact ID
- Add dependencies into pom.xml
- Place all java files in src/main/java
- Create input folder with
	- 1991.csv for ClimateAnalysisNoCombiner,ClimateAnalysisCombiner,ClimateAnalysisInMapperCombiner
	- 1880.csv - 1889.csv (10 files) for ClimateAnalysis_SecondarySort
- Copy config folder from where you installed hadoop and place it under your main project directory
- config, input pom.xml, Makefile, src, should be in the same project directory
- Run the commands interminal of IDE
	- make switch-standalone
	- make alone
- Output folder contains part files generated

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
- Navigate to current java project directory on terminal
- Upload data input to AWS using -
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
- The folder SourceCode contains 4 folders each for each java program
	-ClimateAnalysis_Part1 --> ClimateAnalysisNoCombiner.java
	-ClimateAnalysis_Part1_1 --> ClimateAnalysisCombiner
	-ClimateAnalysis_Part1_2 --> ClimateAnalysisInMapperCombiner
	-ClimateAnalysis_Part2 --> ClimateAnalysis_SecondarySort
- ClimateDataType -- Accumulation Data structure (for all programs)
- CompositeKey -- Composite Key for SecondarySort Program

- The folder SysLog
	- syslogcombiner --> Syslog Combiner
	- sysloginmapper --> Syslog  InMapper
	- syslognocombiner --> Syslog No Combiner
	- syslogsecondarysort --> Syslog Secondary Sort

- The folder Output
	- combiner --> part-r-00000 - part-r-00008 // 9 files
	- inmapper --> part-r-00000 - part-r-00008 // 9 files
	- nocombiner --> part-r-00000 - part-r-00008 // 9 files
	- secsort --> part-r-00000 - part-r-00008 // 5 files



