# ReadME #

##

###Config###

You will first need to setup MarklogicJobRepository by following the instructions [here](https://github.com/marklogic-community/marklogic-spring-batch/wiki/MarkLogicJobRepository). Once you've deployed to your local Marklogic server proceed.

Now go to ```/src/main/resources/job.properties``` and note the config and make any appropriate changes. The first 5 lines refer to the Marklogic Database that you will be be writing content to, lines 7 to 11 refer to the MarklogicJobRepository that you should have just deployed.

Now note the gradle config in ```build.gradle``` At the bottom of the file are 2 sample jobs that you can run.

###Run###

Running ```./gradlew runYourJob``` will run the spring batch job in ```src/main/java/YourJobConfig.java``` which will insert 100 simple json documents into your target Marklogic database.

Running ```./gradlew runMyDirectoryJob``` will run the spring batch job in ```src/main/java/MyImportDocsExtractTextJob.java``` which will read the 2 files in ```sampleInput/``` parse them and insert xhtml representations into marklogic using apache tika.