import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//Reducer:
//
//  The Reducer emits the Keys with only one Value in its Value List
//
//  Reducer Input:
//
//  Key  Value List: 2 {1}
//  Key  Value List: 3 {1}
//  Key  Value List: 4 {1, 1}
//  Key  Value List: 5 {1}
//  Key  Value List: 6 {1, 1, 1}
//  Key  Value List: 7 {1}
//  Key  Value List: 8 {1, 1}
//  Key  Value List: 9 {1, 1}
//  Key  Value List: 10 {1, 1, 1}
//
//  Reducer Output:
//
//  2
//  3
//  5
//  7
public class PrimeNumberReducer extends MapReduceBase implements
    Reducer<LongWritable, ByteWritable, LongWritable, NullWritable>
{
  @Override
  public void reduce(LongWritable key, Iterator<ByteWritable> values,
      OutputCollector<LongWritable, NullWritable> output, Reporter reporter) throws IOException
  {
    int count = 0;
    while (values.hasNext())
    {
      values.next();

      count++;
      if (count > 1)
      {
        break;
      }
    }
    if (count == 1)
    {
      output.collect(key, NullWritable.get());
    }
  }
}
