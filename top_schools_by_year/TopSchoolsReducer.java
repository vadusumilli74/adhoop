import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TopSchoolsReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
  // Concatenate School Details with the same API
  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {
    String value = "";
    while (values.hasNext())
    {
      value += " AND " + values.next().toString();
    }
    value = value.substring(5);
    output.collect(key, new Text(value));
  }
}
