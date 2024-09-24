## What is this project
This is a simple project to which will read a file from given parameter and print in the console

## How to use it 
Run the ```mvn clean install``` in the project and generate the target folder



## How to submit the job to spark
* Download the spark from the spark [official website](https://spark.apache.org/downloads.html)
* Navigate to the path ```spark-3.4.0-bin-hadoop3\bin```
* Run below command to submit your spark job written in java to spark locally
  * ```./spark-submit --class com.kd.main.ReadToDataset  ~/git/spark/spark-test/spark-job/target/spark-job-1.0-SNAPSHOT.jar ~/git/spark/spark-test/spark-job/inventory.csv```
    * In above command we will be utilizing the ```spark-submit``` script provided to us by spark
    * Second parameter will be the ```--class com.kd.main.ReadToDataset``` which is the class we are going to execute. This class should have a main method implemented with the logic you want to excute
    * Third parameter is the jar file we want to execute ```~/git/spark/spark-test/spark-job/target/spark-job-1.0-SNAPSHOT.jar```
    * Fourth parameter is the file we need to read in our script ```~/git/spark/spark-test/spark-job/inventory.csv```. This parameter will be accessed as arg[0] in the main class. if you have more than 1 parameter seperated by spaces and can be accessed in java program with arg[x] 