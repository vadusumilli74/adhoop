import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AlgorithmReducer extends MapReduceBase implements Reducer<FromNodeKey, ToNodeValues, FromNodeKey, ToNodeValues>
{

  @Override
  public void reduce(FromNodeKey key, Iterator<ToNodeValues> values, OutputCollector<FromNodeKey, ToNodeValues> output, Reporter reporter)
      throws IOException
  {
    ToNodeValues newValue = new ToNodeValues();
    while (values.hasNext())
    {
      Iterator<ToNodeValue> iterator = values.next().listIterator();
      while(iterator.hasNext())
      {
        newValue.add(iterator.next());
      }
    }
    
    output.collect(key, newValue);
  }
}
