# COMP5349A1

###
Analyse big dataset with hadoop mapreduce

Refer to assignment1_handout.pdf for detailed requirement
 
##How to run
###requirement
Hadoop 2.6.0  
###Steps 
1. create a hdfs dirctory in your hdfs home named *place* and upload the [place.txt](http://pan.baidu.com/s/1jG27lQ2) into it
2. create another hdfs directory in your hdfs home named *photo* and upload [n01.txt](http://pan.baidu.com/s/1mgkSl1u) into it
3. set A1_HOME environment variable to store the intermidiate output for each jobs
4. In the pom.xml directory : 
   mvn package
5. cd to task1.sh and task2.sh for each tasks,making sure the scripts stay in the same directory as the MRDriverTask1.class (MRDriverTask2.class)
6. pass an integer argument to the task1.sh (or task2.sh) indicating the job start from.for the first time,the argument is always 1


