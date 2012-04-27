import java.util.Hashtable;

import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class OnTimeArrivalPartitioner implements Partitioner<Text, Text>
{
  private Hashtable<String, Integer> mSortingOrder;
  
  @Override
  public int getPartition(Text key, Text value, int numReduceTasks)
  {
    int partitions = mSortingOrder.size() / numReduceTasks;
    
    String naturalKey = key.toString().split("#")[0];
    Integer naturalKeyPriority = (Integer)mSortingOrder.get(naturalKey);
    int partition = naturalKeyPriority.intValue() / partitions;

    return (partition >= numReduceTasks ? numReduceTasks - 1 : partition);
  }

  public void configure(JobConf job)
  {
    mSortingOrder = new Hashtable<String, Integer>();
    mSortingOrder.put(OnTimeArrivalMapper.TITLE, new Integer(0));
    mSortingOrder.put(OnTimeArrivalMapper.ON_TIME, new Integer(1));
    mSortingOrder.put(OnTimeArrivalMapper.AIR_CARRIER_DELAY, new Integer(2));
    mSortingOrder.put(OnTimeArrivalMapper.WEATHER_DELAY, new Integer(3));
    mSortingOrder.put(OnTimeArrivalMapper.NATIONAL_AVIATION_SYSTEM_DELAY, new Integer(4));
    mSortingOrder.put(OnTimeArrivalMapper.SECURITY_DELAY, new Integer(5));
    mSortingOrder.put(OnTimeArrivalMapper.AIRCRAFT_ARRIVING_LATE, new Integer(6));
    mSortingOrder.put(OnTimeArrivalMapper.CANCELLED, new Integer(7));
    mSortingOrder.put(OnTimeArrivalMapper.DIVERTED, new Integer(8));
    mSortingOrder.put(OnTimeArrivalMapper.TOTAL_OPERATIONS, new Integer(9));
  }
}