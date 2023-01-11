
# GPDR Compliance

A command-line application that reads a CSV file, performs a specified action (delete or hash) on a client personal 
information.




## Installation

Install my-project with npm

```bash
  git clone https://github.com/GlobeCitizen/scala-tp
  cd scala-tp
  sbt package
```
    
## Usage/Examples
Run the program with the following arguments:

* -i, --clientId: a long property that specifies the client ID
* -a, --action: a string property that specifies the action to be performed. Accepted values are "delete" and "hash"
* -p, --hdfsPath: a string property that specifies the HDFS path of the input file
* -f, --fileName: a string property that specifies the name of the input file

Example:
```bash
spark-submit --class Main target/scala-2.X/myApp-assembly-0.1.jar  -i 12345 -a delete -p hdfs://localhost:9000/ -f input.csv

```


## Dependencies
This program uses the following dependencies:

* spark-core 2.4.8
* spark-sql 2.4.8
* scopt 4.1.0
* spray-json 1.3.6
