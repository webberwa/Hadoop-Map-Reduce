#!/usr/bin/env bash

# Compile
hadoop com.sun.tools.javac.Main ./src/main/java/com/usc/webber/WordCount.java

# Jar
#jar cf ./WordCount.jar ./src/main/java/com/usc/webber/WordCount*.class
jar cmf ./src/main/java/META-INF/MANIFEST.MF ./WordCount.jar ./src/main/java/com/usc/webber/WordCount*.class

rm -rf ./devoutput

# Format
#hadoop fs -mkdir -p /user/webber

# Copy to HDFS   (make sure name nodes / data nodes are setup)
hadoop fs -copyFromLocal -f ./WordCount.jar WordCount.jar
hadoop fs -copyFromLocal -f ./devdata devdata

# Run
hadoop jar ./WordCount.jar com.usc.webber.WordCount ./devdata ./devoutput



