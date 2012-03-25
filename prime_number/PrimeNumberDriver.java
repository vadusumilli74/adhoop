import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Generate Prime Numbers between 2 and N using:
//
// custom Input Format
// custom Input Split
// custom Record Reader
//
public class PrimeNumberDriver extends Configured implements Tool
{
  public static final long MAXIMUM_INTEGER_VALUE     = Integer.MAX_VALUE;
  public static final long MAXIMUM_INTEGER_VALUE_MID = MAXIMUM_INTEGER_VALUE / 2;

  @Override
  public int run(String[] args) throws Exception
  {

    if (args.length != 1)
    {
      System.out.printf("Usage: %s [generic options] <output dir>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    JobConf conf = new JobConf(getConf(), PrimeNumberDriver.class);
    conf.setJobName(this.getClass().getName());

    conf.setMapperClass(PrimeNumberMapper.class);
    conf.setPartitionerClass(PrimeNumberPartitioner.class);
    conf.setReducerClass(PrimeNumberReducer.class);

    conf.setNumReduceTasks(5);

    conf.setInputFormat(PrimeNumberInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(ByteWritable.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(NullWritable.class);

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));

    JobClient.runJob(conf);

    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new PrimeNumberDriver(), args);
    System.exit(exitCode);
  }
}
