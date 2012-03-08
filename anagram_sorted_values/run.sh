javac -classpath $HADOOP_HOME/hadoop-core.jar *.java
jar cvf anagram.jar *.class
hadoop fs -rmr outputAnagram
hadoop jar anagram.jar AnagramSortedValuesDriver inputAnagram outputAnagram
rm -f output.txt
hadoop fs -cat 'outputAnagram/part-*' > output.txt