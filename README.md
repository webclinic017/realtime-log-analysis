# Realtime Log Analysis 
<img src="https://img.shields.io/badge/Project%20Status%20-Work%20in%20Progress-green"></img>

This project is an implementation of common practice log analysis using python and other various technologies. 


In this project we extract logs from NASA and process them using the streaming solutions provided by Apache Spark and Apache Kafka. 
Beyond processing we provide a set of monitoring interfaces for developers. 

See below for the general architecture. 

![Log Analysis Project](https://user-images.githubusercontent.com/91840749/150691446-b64321e0-84e4-4809-918f-4b617632c3b2.png)


# Key Technologies Used
* Prefect 
* Apache Kafka 
* Apache Spark
* Apache Cassandra 
* Metabase 
* GCP


# Setup / Installation

1. Clone this repository in the directory of your choice.
2. Run `pulumi up`. This will provision your infrastructure on the cloud. 
3. Navigate to Metabase to interact with the log data. 



# Run Test Suite

`pytest tests`


