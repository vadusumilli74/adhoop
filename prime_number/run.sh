rm -rf *.class
rm -rf *.jar
rm -f output.txt
hadoop fs -rmr outputPrimeNumber

javac -classpath /usr/lib/hadoop/hadoop-core.jar *.java
jar cvf prime.jar *.class
hadoop jar prime.jar PrimeNumberDriver outputPrimeNumber
hadoop fs -cat 'outputPrimeNumber/part-*' > output.txt
