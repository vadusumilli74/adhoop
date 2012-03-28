import java.io.IOException;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MatrixMultiplicationOneReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{

  private int stepNumber = 0;

  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {
    if (stepNumber == 0)
    {
      ArrayList<String> valuesGroupedByM = new ArrayList<String>();
      ArrayList<String> valuesGroupedByN = new ArrayList<String>();

      while (values.hasNext())
      {
        String value = values.next().toString();
        (value.startsWith(MatrixMultiplicationOneDriver.MATRIX_NAME_M) ? valuesGroupedByM : valuesGroupedByN).add(value);
      }

      Iterator<String> iteratorM = valuesGroupedByM.iterator();
      while (iteratorM.hasNext())
      {
        String[] tokensM = iteratorM.next().split(MatrixMultiplicationOneDriver.SEPARATOR);

        Iterator<String> iteratorN = valuesGroupedByN.iterator();
        while (iteratorN.hasNext())
        {
          String[] tokensN = iteratorN.next().split(MatrixMultiplicationOneDriver.SEPARATOR);

          output.collect(
              key,
              new Text(tokensM[1] + MatrixMultiplicationOneDriver.SEPARATOR + tokensN[1] + MatrixMultiplicationOneDriver.SEPARATOR
                  + (Double.parseDouble(tokensM[2]) * Double.parseDouble(tokensN[2]))));
        }
      }
    }
    else
    {
      Double newValue = new Double(0);
      while (values.hasNext())
      {
        newValue += Double.parseDouble(values.next().toString());
      }
      output.collect(key, new Text(newValue.toString()));
    }
  }

  @Override
  public void configure(JobConf job)
  {
    stepNumber = job.getInt(MatrixMultiplicationOneDriver.STEP_NUMBER, 0);
  }
}
