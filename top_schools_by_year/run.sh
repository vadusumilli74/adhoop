if [ $# -ne 2 ] || [ $2 != 'E' -a $2 != 'M' -a $2 != 'H' ]
then
  echo "Usage: $0 YEAR SCHOOL_TYPE"
  echo "Example: $0 2011 E"
  echo "SCHOOL_TYPE = E | M | H"
  exit 1
fi

javac -classpath $HADOOP_HOME/hadoop-core.jar *.java
jar cvf TopSchools.jar *.class
hadoop fs -rmr outputTopSchools
hadoop jar TopSchools.jar TopSchoolsDriver inputSchool/api/$1api.txt outputTopSchools $1 $2
rm -f $1_$2_top_schools.txt
hadoop fs -cat 'outputTopSchools/part-*' > $1_$2_top_schools.txt
head -10 $1_$2_top_schools.txt
