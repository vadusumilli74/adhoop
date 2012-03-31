import java.io.IOException;

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

  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {
    Hashtable<String, String> mHashTable = new Hashtable<String, String>();
    Hashtable<String, String> nHashTable = new Hashtable<String, String>();
    
    while (values.hasNext())
    {
      String value = values.next().toString();
      String[] tokens = value.split(MatrixMultiplicationOneDriver.SEPARATOR);
      if (tokens[0].equalsIgnoreCase(MatrixMultiplicationOneDriver.MATRIX_NAME_M))
      {
        mHashTable.put(tokens[1], value);
      }
      else
      {
        nHashTable.put(tokens[1], value);
      }
    }

    double value = 0;
    for (int index = 0; index < mHashTable.size(); index++)
    {
      value = value + Double.parseDouble(mHashTable.get(Integer.toString(index)).split(MatrixMultiplicationOneDriver.SEPARATOR)[2])
          * Double.parseDouble(nHashTable.get(Integer.toString(index)).split(MatrixMultiplicationOneDriver.SEPARATOR)[2]);
    }
    
    output.collect(key, new Text(Double.toString(value)));
  }
}
