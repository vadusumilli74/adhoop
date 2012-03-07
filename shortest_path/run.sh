javac -classpath $HADOOP_HOME/hadoop-core.jar *.java
rm -f algorithm.jar
jar cvf algorithm.jar *.class

rm -f output*.txt

hadoop fs -rmr 'outputAlgorithm*'
hadoop jar algorithm.jar AlgorithmDriver inputAlgorithm/shortest_path.txt outputAlgorithm
rm -f output.txt
hadoop fs -cat 'outputAlgorithm/part-*' > output.txt
more output.txt

<<COMMENT
maximumIterations=6
next=1;
for ((  i = 1 ;  i < $maximumIterations;  i++  ))
do
  next=$((next + 1))
  echo "MapReduce Run: $next"
  hadoop fs -rmr inputAlgorithm/shortest_path_$i.txt
  hadoop fs -put output_$i.txt inputAlgorithm/shortest_path_$i.txt
  hadoop fs -rmr outputAlgorithm
  hadoop jar algorithm.jar AlgorithmDriver inputAlgorithm/shortest_path_$i.txt outputAlgorithm
  rm -f output_$next.txt
  hadoop fs -cat 'outputAlgorithm/part-*' > output_$next.txt
  more output_$next.txt
done

echo "Intermediate Output Files"
for ((  i = 1 ;  i <= $maximumIterations;  i++  ))
do
  ls -al output_$i.txt
  more output_$i.txt
done
COMMENT