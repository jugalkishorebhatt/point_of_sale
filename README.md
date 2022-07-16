# point of sale

## Table of Content

* Introduction
* Prerequisite
* Architecture
* Application Overview
* Github link
* Execution
* Unit Testing


### Introduction:
POS application receives transactional data from different retails stores. 

### Prerequisite:
Below are the list of softwares required to run the application:
1) Spark
2) Python
3) Docker

### Architecture: 
![POS APP!](https://user-images.githubusercontent.com/18581106/179350169-36f80089-1248-4997-a5e2-a0665113e134.jpeg)

### Application Overview:

#### <u>posTransJob</u>
Main application wihch triggers necessary functions and upload the config file. Which list on how the application should execute.

#### <u>config.yaml</u>
Essential parameters required are the module and class name. With other user defined parameters required to make the users part of application interactive.

#### <u>posTrans</u>
Instanciates dependent application dynamically depending on the module and the class name provided in the config.yaml file

#### <u>comon</u>
List of functions which are common across the application.

#### <u>posTransAgg</u>
POS application targets in finding the customers credit card transaction(spending) patterns and behaviour at customer level(or different life stage).

### Github link:
Below is the application link:

https://github.com/jugalkishorebhatt/point_of_sale/

### Execution:
POS Application being build on pyspark. Below is the snippet of how the application is run. Users are bound to add dependent .py files required to run the specific part of their application. 

Command:

spark-submit --master "local[*]" --py-files posTransAgg.py,posTrans.py,common.py posTransJob.py --config config.yaml

### Unit Testing:
Below is the command to run the unit test:

python3.6 -m unittest run test.PosTestCase
