import java.io.IOException;

import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GenerateRowNumberMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
  String mapInputFile  = "";
  long   mapInputStart = 0;

  @Override
  public void configure(JobConf conf)
  {
    String[] keyTokens = conf.get("map.input.file", "XYZ").split(", ");
    String[] fileNameTokens = keyTokens[0].split("/");
    mapInputFile = fileNameTokens[fileNameTokens.length - 1];

    mapInputStart = conf.getLong("map.input.start", 0);
  }

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {
    output.collect(new Text(mapInputFile + ", " + mapInputStart + ", " + key.toString()), value);
  }
}
