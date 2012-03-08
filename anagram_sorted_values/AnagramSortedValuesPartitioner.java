import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Text;

public class AnagramSortedValuesPartitioner implements Partitioner<Text, Text>
{

  // Key: Sorted Characters of Word # Word
  // Partition Based On: Sorted Characters of Word
  // This will ensure all the words that are anagrams of each other are sent to the same partitioner.
  @Override
  public int getPartition(Text key, Text value, int numReduceTasks)
  {
    String naturalKey = key.toString().split("#")[0];

    return (naturalKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
  }

  public void configure(JobConf job)
  {

  }
}