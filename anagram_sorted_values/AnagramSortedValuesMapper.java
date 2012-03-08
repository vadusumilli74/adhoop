import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AnagramSortedValuesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{

  // Input Key: Line Number
  // Input Value: Word
  //
  // Output Key: Sorted Characters of Word # Word
  // Output Value: Word
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {
    String wordString = value.toString().trim();
    char[] wordArray = wordString.toCharArray();
    Arrays.sort(wordArray);
    String wordStringSorted = String.valueOf(wordArray);
    output.collect(new Text(wordStringSorted + "#" + wordString), new Text(wordString));
  }
}
