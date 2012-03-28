rm -f *.class
rm -f *.jar

javac -classpath /usr/lib/hadoop/hadoop-core.jar *.java
jar cvf matrix_multiplication_one.jar *.class

hadoop jar matrix_multiplication_one.jar MatrixMultiplicationOneDriver M.txt N.txt