import java.io.IOException;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AlgorithmMapper extends MapReduceBase implements
    Mapper<FromNodeKey, ToNodeValues, FromNodeKey, ToNodeValues>
{
  private int jobNumber = 0;
  private int nodeNumber = 0;
  
  private ToNodeValues emptyValue = new ToNodeValues();

  @Override
  public void map(FromNodeKey key, ToNodeValues value, OutputCollector<FromNodeKey, ToNodeValues> output,
      Reporter reporter) throws IOException
  {
    reporter.getCounter(AlgorithmDriver.CUSTOM_COUNTERS, AlgorithmDriver.JOB_NUMBER).setValue(jobNumber);
    reporter.getCounter(AlgorithmDriver.CUSTOM_COUNTERS, AlgorithmDriver.NODE_NUMBER).setValue(nodeNumber);
    
    if (key.nodeWeight() != Integer.MIN_VALUE && !key.discovered())
    {
      FromNodeKey newKey = new FromNodeKey();
      newKey.node(key.node());
      newKey.nodeWeight(key.nodeWeight());
      newKey.discovered(true);
      newKey.path(key.path());
      newKey.addPath(key.node());
      output.collect(newKey, value);

      if (value.size() > 0)
      {
        for (int index = 0; index < value.size(); index++)
        {
          ToNodeValue toNodeValue = value.get(index);
          if (!newKey.inPath(toNodeValue.node()))
          {
            FromNodeKey newKeyAdjacent = new FromNodeKey();
            newKeyAdjacent.node(toNodeValue.node());
            newKeyAdjacent.nodeWeight(key.nodeWeight() + toNodeValue.edgeWeight());
            newKeyAdjacent.discovered(false);
            newKeyAdjacent.path(newKey.path());
            output.collect(newKeyAdjacent, emptyValue);

            reporter.incrCounter(AlgorithmDriver.CUSTOM_COUNTERS, AlgorithmDriver.UNDISCOVERED_NODES_COUNT, 1);
          }
        }
      }
    }
    else
    {
      output.collect(key, value);
    }
  }
  
  @Override
  public void configure(JobConf job)
  {
    jobNumber = job.getInt(AlgorithmDriver.JOB_NUMBER, Integer.MAX_VALUE);
    nodeNumber = job.getInt(AlgorithmDriver.NODE_NUMBER, Integer.MAX_VALUE);
  }
}
