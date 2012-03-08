import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AnagramReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{

  // Input Key: Sorted Characters of Word
  // Input Value: [First Anagram Word, Second Anagram Word, Third Anagram Word...]
  //
  // Output Key: First Anagram Word
  // Output Value: Second Anagram Word, Third Anagram Word...
  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {

    String newKey = "";
    String newValue = "";
    while (values.hasNext())
    {
      if (newKey.length() == 0)
      {
        newKey = values.next().toString();
      }
      else
      {
        newValue += ", " + values.next().toString();
      }
    }

    if (newValue.length() > 0)
    {
      newValue = newValue.substring(2);
      output.collect(new Text(newKey), new Text(newValue));
    }

  }
}
