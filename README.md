# Polysomnographic-Data-
Term Paper 
We will conduct a tempo-spatial data analysis using the Polysomnographic Sleep dataset. The dataset consists of demographic and physiological data from 40 patients. Each patient had two nights of EEG signals and other attributes such as apnea counts and oxygen desaturation index. The goal is to analyze sleep quality patterns over time and compare them across patients. We will process and store the data using Hadoop and Hive, and apply queries to identify how demographic and physiological variables relate to sleep disorders.The insights will be presented through data visualizations, such as maps and charts using Power BI, to show the findings. Sleep disordered breathing (SDB) is a significant health challenge for many people and patterns may help with data driven diagnosis of sleep issues.

Here are the following steps we took:

Connect to the cluster via SSH.

Download the dataset from https://www.kaggle.com/datasets/yfrite/polysom?resource=download.

Import the zip file via SCP.

unzip Polysom.zip -d polysom_raw # Extract the dataset into a new folder in your home directory

ls polysom_raw # Verify the dataset extracted and locate patients.csv

hdfs dfs -mkdir -p /tmp/polysom_tabular # Create a project folder in HDFS for the tabular data

hdfs dfs -put ~/polysom_raw/patients.csv /tmp/polysom_tabular/ # Move the CSV from local Linux filesystem to HDFS

hdfs dfs -ls /tmp # Confirm the HDFS directory was created

hdfs dfs -ls /tmp/polysom_tabular # Ensure patients.csv is inside the HDFS directory

beeline # Open the Hive CLI for creating tables and running queries

CREATE DATABASE IF NOT EXISTS sleep_team_db; -- Create a shared database for the project USE sleep_team_db; -- Use the project database

CREATE EXTERNAL TABLE patients_raw ( user_id INT, night_id INT, age INT, sex STRING, height INT, weight INT, pulse INT, bpsys_bpdia STRING, ODI DOUBLE, NAp INT, NHyp INT, AI DOUBLE, HI DOUBLE, AHI DOUBLE ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/tmp/polysom_tabular' TBLPROPERTIES ("skip.header.line.count"="1");
