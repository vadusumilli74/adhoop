rm -f *.class
rm -f *.jar

javac -classpath /usr/lib/hadoop/hadoop-core.jar *.java
jar cvf find_a_path.jar *.class

hadoop jar find_a_path.jar AlgorithmDriver input.txt output.txt inputFindAPath outputFindAPath
more output.txt