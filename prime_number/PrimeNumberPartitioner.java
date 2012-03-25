import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class PrimeNumberPartitioner implements Partitioner<LongWritable, ByteWritable>
{
  @Override
  public int getPartition(LongWritable key, ByteWritable value, int numReduceTasks)
  {
    long blockSize = PrimeNumberDriver.MAXIMUM_INTEGER_VALUE / numReduceTasks;

    return (int) (key.get() / blockSize) % numReduceTasks;
  }

  public void configure(JobConf job)
  {
  }
}