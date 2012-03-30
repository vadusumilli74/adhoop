rm -rf *.class
rm -rf *.jar
rm -f output.txt

hadoop fs -rmr inputGenerateRowNumber
hadoop fs -rmr outputGenerateRowNumber

javac -classpath /usr/lib/hadoop/hadoop-core.jar *.java
jar cvf GenerateRowNumber.jar *.class

hadoop fs -put M.txt inputGenerateRowNumber/M.txt
hadoop fs -put N.txt inputGenerateRowNumber/N.txt
hadoop jar GenerateRowNumber.jar GenerateRowNumberDriver inputGenerateRowNumber outputGenerateRowNumber
