import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

public class AlgorithmPartitioner implements Partitioner<Text, Text>
{
  @Override
  public int getPartition(Text key, Text value, int numReduceTasks)
  {
    String compositeKey = key.toString();

    String[] nodeDetails = compositeKey.substring(1, compositeKey.length() - 1).split(",");

    int returnValue = (nodeDetails[AlgorithmMapper.INDEX_SOURCE_NODE_NUMBER].hashCode() & Integer.MAX_VALUE)
        % numReduceTasks;

    return returnValue;
  }

  public void configure(JobConf job)
  {
  }
}