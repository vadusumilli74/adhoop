import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

public class GenerateRowNumberPartitioner implements Partitioner<Text, Text>
{
  @Override
  public int getPartition(Text key, Text value, int numReduceTasks)
  {
    String [] wcTokens = key.toString().split(", ");
    
    int returnValue = (wcTokens[0].hashCode() & Integer.MAX_VALUE) % numReduceTasks;

    return returnValue;
  }

  public void configure(JobConf job)
  {
  }
}