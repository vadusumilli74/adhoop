import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AnagramReducer extends MapReduceBase implements
    Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {

    String newKey = "";
    String anagrams = "";
    int count = 0;
    while (values.hasNext()) {
      count++;
      if (count == 1)
      {
       	newKey = values.next().toString();
      }
      else
      {
        anagrams += values.next().toString() + ",";
      }
    }
    
    if (count > 1)
    {
      anagrams = anagrams.substring(0, anagrams.length() - 1);
      output.collect(new Text(newKey), new Text(anagrams));
    }

  }
}
