rm -f *.class
rm -f *.jar
rm -f on_time_arrival.txt

hadoop fs -rmr outputFAAOnTimeArrival

javac -classpath /usr/lib/hadoop/hadoop-core.jar *.java
jar cvf faa_on_time_arrival.jar *.class

hadoop jar faa_on_time_arrival.jar OnTimeArrivalDriver inputFAA outputFAAOnTimeArrival
hadoop fs -cat 'outputFAAOnTimeArrival/part-*' > on_time_arrival.txt