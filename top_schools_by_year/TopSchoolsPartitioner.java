import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class TopSchoolsPartitioner implements Partitioner<Text, Text>
{
  // Key: API (Range = 0-1000)
  // Total Sort: 000 - 0250 = Reducer 3
  //             251 - 0500 = Reducer 2
  //             501 - 0750 = Reducer 1
  //             751 - 1000 = Reducer 0
  @Override
  public int getPartition(Text key, Text value, int numReduceTasks)
  {
    Integer api = Integer.valueOf(key.toString());

    return numReduceTasks - 1 - ((api / TopSchoolsDriver.API_BLOCK_SIZE) % numReduceTasks);
  }

  public void configure(JobConf job)
  {

  }
}