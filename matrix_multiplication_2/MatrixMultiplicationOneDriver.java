import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.net.URI;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.mapred.lib.MultipleOutputs;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatrixMultiplicationOneDriver extends Configured implements Tool
{
  public static final String    ROW_COUNTERS    = "ROW_COUNTERS";
  public static final String    COLUMN_COUNTERS = "COLUMN_COUNTERS";
  public static final String    SEPARATOR       = ", ";
  public static final String    STEP_NUMBER     = "STEP_NUMBER";
  public static final String    MATRIX_NAME_M   = "M";
  public static final String    MATRIX_NAME_N   = "N";

  public static final String[] nameMatrices    = { MATRIX_NAME_M, MATRIX_NAME_N };
  private static final String   INPUT_FILE      = "input.txt";
  private static final String   OUTPUT_FILE     = "output.txt";

  String[]                      inputFileMatrices;
  String                        inputPath       = "";
  String                        outputPath      = "";
  long[]                        rowCounts       = { 0, 0 };
  long[]                        columnCounts    = { 0, 0 };

  @Override
  public int run(String[] args) throws Exception
  {
    RunningJob job;
    
    if (args.length != 2)
    {
      System.out.printf("Usage: %s [generic options] <Matrix M> <Matrix N>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputFileMatrices = new String[2];
    inputFileMatrices[0] = args[0];
    inputFileMatrices[1] = args[1];

    inputPath = "input" + this.getClass().getName();
    outputPath = "output" + this.getClass().getName();

    String currentInputPath = inputPath;
    String currentOutputPath = inputPath;

    JobConf prepareJobConfiguration = new JobConf(getConf(), MatrixMultiplicationOneDriver.class);

    // Prepare the Input Directory
    Path inputFileDeletePath = new Path(inputPath);
    FileSystem inputFileDeleteFileSystem = inputFileDeletePath.getFileSystem(prepareJobConfiguration);
    inputFileDeleteFileSystem.delete(inputFileDeletePath, true);

    Path outputFileDeletePath = new Path(outputPath);
    FileSystem outputFileDeleteFileSystem = outputFileDeletePath.getFileSystem(prepareJobConfiguration);
    outputFileDeleteFileSystem.delete(outputFileDeletePath, true);

    LocalFileSystem localFileSystem = new LocalFileSystem();
    localFileSystem.initialize(new URI(""), prepareJobConfiguration);
    for (int index = 0; index < inputFileMatrices.length; index++)
    {
      Path localPath = new Path(inputFileMatrices[index]);
      Path remotePath = new Path(inputPath + "/" + nameMatrices[index] + ".txt");
      if (FileUtil.copy(localFileSystem, localPath, remotePath.getFileSystem(prepareJobConfiguration), remotePath,
          false, false, prepareJobConfiguration))
      {
        MultipleOutputs.addMultiNamedOutput(prepareJobConfiguration, nameMatrices[index], TextOutputFormat.class,
            Text.class, Text.class);
      }
      else
      {
        System.err.printf("ERROR: Failed to copy " + nameMatrices[index] + " in to HDFS.");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
      }
    }

    // Generate Row Numbers
    prepareJobConfiguration.setJobName(this.getClass().getName());

    prepareJobConfiguration.setMapperClass(GenerateRowNumberMapper.class);
    prepareJobConfiguration.setOutputValueGroupingComparator(GenerateRowNumberOutputValueGroupingComparator.class);
    prepareJobConfiguration.setOutputKeyComparatorClass(GenerateRowNumberOutputKeyComparator.class);
    prepareJobConfiguration.setPartitionerClass(GenerateRowNumberPartitioner.class);
    prepareJobConfiguration.setReducerClass(GenerateRowNumberReducer.class);

    prepareJobConfiguration.setMapOutputKeyClass(Text.class);
    prepareJobConfiguration.setMapOutputValueClass(Text.class);

    prepareJobConfiguration.setOutputKeyClass(Text.class);
    prepareJobConfiguration.setOutputValueClass(Text.class);

    currentInputPath = inputPath;
    currentOutputPath = outputPath + "/0";

    FileInputFormat.setInputPaths(prepareJobConfiguration, new Path(currentInputPath));
    FileOutputFormat.setOutputPath(prepareJobConfiguration, new Path(currentOutputPath));

    job = JobClient.runJob(prepareJobConfiguration);

    for (int index = 0; index < nameMatrices.length; index++)
    {
      rowCounts[index] = job.getCounters()
          .findCounter(MatrixMultiplicationOneDriver.ROW_COUNTERS, nameMatrices[index]).getValue();
      columnCounts[index] = job.getCounters()
          .findCounter(MatrixMultiplicationOneDriver.COLUMN_COUNTERS, nameMatrices[index]).getValue();
    }
    
    if (columnCounts[0] != rowCounts[1])
    {
      System.err.printf("M Column Count[" + columnCounts[0] + "] != N Row Count[" + rowCounts[1] + "]");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    // Algorithm
    JobConf conf = new JobConf(getConf(), MatrixMultiplicationOneDriver.class);
    conf.setJobName(this.getClass().getName());
    for (int index = 0; index < nameMatrices.length; index++)
    {
      conf.setLong(MatrixMultiplicationOneDriver.ROW_COUNTERS + SEPARATOR + nameMatrices[index], rowCounts[index]);
      conf.setLong(MatrixMultiplicationOneDriver.COLUMN_COUNTERS + SEPARATOR + nameMatrices[index], columnCounts[index]);
    }

    conf.setMapperClass(MatrixMultiplicationOneMapper.class);
    conf.setReducerClass(MatrixMultiplicationOneReducer.class);

    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    currentInputPath = currentOutputPath;
    currentOutputPath = outputPath + "/1";

    FileInputFormat.setInputPaths(conf, new Path(currentInputPath));
    FileOutputFormat.setOutputPath(conf, new Path(currentOutputPath));

    job = JobClient.runJob(conf);

    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new MatrixMultiplicationOneDriver(), args);
    System.exit(exitCode);
  }
}
