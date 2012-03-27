import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;

public class AlgorithmPartitioner implements Partitioner<FromNodeKey, ToNodeValues>
{
  @Override
  public int getPartition(FromNodeKey key, ToNodeValues value, int numReduceTasks)
  {
    int returnValue = (key.node() & Integer.MAX_VALUE) % numReduceTasks;

    return returnValue;
  }

  public void configure(JobConf job)
  {
  }
}