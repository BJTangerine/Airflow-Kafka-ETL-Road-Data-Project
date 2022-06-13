#!/bin/bash

# setup:

# Getting Apache Kafka.
wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz 
tar -xzf kafka_2.12-2.8.0.tgz 

# to enable usage of Python scripts.
pip3 install kafka-python && pip3 install mysql-connector-python

# MySQL setup, creating database & table.
echo 'CREATE DATABASE tolldata;' | mysql --host=host_here --port=port_here --user=root --password=pswrd_here
echo 'create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);' | mysql --host=host_here --port=port_here --user=root --password=pswrd_here --database=tolldata