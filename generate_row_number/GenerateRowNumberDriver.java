import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.lib.MultipleOutputs;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenerateRowNumberDriver extends Configured implements Tool
{

  @Override
  public int run(String[] args) throws Exception
  {

    if (args.length != 2)
    {
      System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Path inputPath = new Path(args[0]);
    
    JobConf conf = new JobConf(getConf(), GenerateRowNumberDriver.class);
    conf.setJobName(this.getClass().getName());

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    FileSystem inputFileSystem = inputPath.getFileSystem(conf);
    FileStatus[] fileStatusValues = inputFileSystem.listStatus(inputPath);
    for (FileStatus fileStatus : fileStatusValues)
    {
      String name = fileStatus.getPath().getName();
      if (name.indexOf(".") >= 0)
      {
        name = name.substring(0, name.indexOf("."));
      }
      name = name.replaceAll("[^a-zA-Z0-9]+","");
      MultipleOutputs.addMultiNamedOutput(conf, name, TextOutputFormat.class, Text.class, Text.class);
    }
    
    conf.setMapperClass(GenerateRowNumberMapper.class);
    conf.setOutputValueGroupingComparator(GenerateRowNumberOutputValueGroupingComparator.class);
    conf.setOutputKeyComparatorClass(GenerateRowNumberOutputKeyComparator.class);
    conf.setPartitionerClass(GenerateRowNumberPartitioner.class);
    conf.setReducerClass(GenerateRowNumberReducer.class);
    
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new GenerateRowNumberDriver(), args);
    System.exit(exitCode);
  }
}
