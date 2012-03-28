import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplicationOneMapper extends MapReduceBase implements
    Mapper<Text, Text, Text, Text>
{
  private int stepNumber = 0;

  @Override
  public void map(Text key, Text value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException
  {
    if (stepNumber == 0)
    {
      output.collect(key, value);
    }
    else
    {
      String[] tokens = value.toString().split(MatrixMultiplicationOneDriver.SEPARATOR);
      String newKey = tokens[0] + MatrixMultiplicationOneDriver.SEPARATOR + tokens[1];
      String newValue = tokens[2];
      
      output.collect(new Text(newKey), new Text(newValue));
    }
  }
  
  @Override
  public void configure(JobConf job)
  {
    stepNumber = job.getInt(MatrixMultiplicationOneDriver.STEP_NUMBER, 0);
  }
}
