# COMP5349A1

###
Analyse big dataset with hadoop mapreduce

Refer to assignment1_handout.pdf for detailed requirement
 
##How to run
###requirement
Hadoop 2.6.0  
###Steps 
1. hdfs dfs -mkdir photo
   upload the data to it with 
2. set A1_HOME environment variable to store the intermidiate output for each jobs
3. In the pom.xml directory : 
   mvn package
4. cd to task1.sh and task2.sh for each tasks,making sure the scripts stay in the same directory as the MRDriverTask1.class (MRDriverTask2.class)
5. pass an integer argument to the task1.sh (or task2.sh) indicating the job start from.for the first time,the argument is always 1


