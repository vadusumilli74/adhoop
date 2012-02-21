javac -classpath $HADOOP_HOME/hadoop-core.jar *.java
jar cvf anagram.jar *.class
hadoop fs -rmr inputAnagram
hadoop fs -mkdir inputAnagram
hadoop fs -put sample_dictionary.txt inputAnagram/dictionary.txt
hadoop fs -rmr outputAnagram
hadoop jar anagram.jar AnagramDriver inputAnagram outputAnagram
rm -f anagrams_unsorted.txt
hadoop fs -get outputAnagram/part-00000 anagrams_unsorted.txt
