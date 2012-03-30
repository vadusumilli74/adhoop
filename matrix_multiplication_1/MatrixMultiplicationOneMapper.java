import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplicationOneMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
  private String matrixName = "";
  private int    stepNumber = 0;

  @Override
  public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
  {
    if (stepNumber == 0)
    {
      String[] values = value.toString().trim().split(" ");
      for (int column = 0; column < values.length; column++)
      {
        if (matrixName.equalsIgnoreCase(MatrixMultiplicationOneDriver.MATRIX_NAME_M))
        {
          output.collect(new Text(Integer.toString(column)), new Text(matrixName + MatrixMultiplicationOneDriver.SEPARATOR + key.toString() + MatrixMultiplicationOneDriver.SEPARATOR + values[column]));
        }
        else
        {
          output.collect(key, new Text(matrixName + MatrixMultiplicationOneDriver.SEPARATOR + Integer.toString(column) + MatrixMultiplicationOneDriver.SEPARATOR + values[column]));
        }
      }
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
  public void configure(JobConf conf)
  {
    stepNumber = conf.getInt(MatrixMultiplicationOneDriver.STEP_NUMBER, 0);
    if (stepNumber == 0)
    {
      String[] fileNameTokens = conf.get("map.input.file", "XYZ").split("/");
      matrixName = fileNameTokens[fileNameTokens.length - 1].substring(0, 1);
    }
  }
}
