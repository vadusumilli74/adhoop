import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatrixMultiplicationOneDriver extends Configured implements Tool
{
  public static final String  SEPARATOR        = ", ";
  public static final String  STEP_NUMBER      = "STEP_NUMBER";
  public static final String  MATRIX_NAME_M    = "M";
  public static final String  MATRIX_NAME_N    = "N";

  private static final String INPUT_FILE       = "input.txt";
  private static final String OUTPUT_FILE      = "output.txt";

  String                      inputFileMatrixM = "";
  String                      inputFileMatrixN = "";
  String                      inputPath        = "";
  String                      outputPath       = "";

  private boolean generateInputFile()
  {
    String[] inputFileMatrices = { inputFileMatrixM, inputFileMatrixN };
    String[] nameMatrices = { MATRIX_NAME_M, MATRIX_NAME_N };
    int[] rowCount = { 0, 0 };
    int[] columnCount = { Integer.MIN_VALUE, Integer.MIN_VALUE };

    boolean returnValue = false;
    boolean exitFunction = false;

    BufferedReader bufferedReader = null;
    Writer writer = null;

    Path writerPath = new Path(inputPath + "/" + INPUT_FILE);
    JobConf writerJobConfiguration = new JobConf();
    try
    {
      FileSystem writerFileSystem = writerPath.getFileSystem(writerJobConfiguration);
      writer = SequenceFile.createWriter(writerFileSystem, writerJobConfiguration, writerPath, Text.class, Text.class);
      for (int index = 0; index < inputFileMatrices.length && !exitFunction; index++)
      {
        bufferedReader = new BufferedReader(new FileReader(new File(inputFileMatrices[index])));
        String line = null;
        while ((line = bufferedReader.readLine()) != null && !exitFunction)
        {
          line = line.trim();
          String[] values = line.split(" ");
          if (columnCount[index] == Integer.MIN_VALUE)
          {
            columnCount[index] = values.length;
          }
          else if (columnCount[index] != values.length)
          {
            exitFunction = true;
            break;
          }
          for (int column = 0; column < columnCount[index]; column++)
          {
            if (index == 0)
            {
              writer.append(new Text(Integer.toString(column)),
                  new Text(nameMatrices[index] + SEPARATOR + Integer.toString(rowCount[index]) + SEPARATOR
                      + values[column]));
            }
            else
            {
              writer.append(new Text(Integer.toString(rowCount[index])), new Text(nameMatrices[index] + SEPARATOR
                  + Integer.toString(column) + SEPARATOR + values[column]));
            }
          }
          rowCount[index]++;
        }
      }

      returnValue = (columnCount[0] == rowCount[1]);
    }
    catch (Exception ex)
    {
      System.out.printf(ex.getMessage(), getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
    }
    finally
    {
      if (bufferedReader != null)
      {
        try
        {
          bufferedReader.close();
        }
        catch (Exception ex)
        {

        }
      }

      if (writer != null)
      {
        try
        {
          writer.close();
        }
        catch (Exception ex)
        {

        }
      }
    }

    return returnValue;
  }

  private void updateOutputFile(String currentOutputPath)
  {
    SequenceFile.Reader reader = null;
    BufferedWriter bufferedWriter = null;
    try
    {
      bufferedWriter = new BufferedWriter(new FileWriter(new File(OUTPUT_FILE)));

      Path readerPath = new Path(currentOutputPath);
      JobConf readerJobConfiguration = new JobConf();
      FileSystem readerFileSystem = readerPath.getFileSystem(readerJobConfiguration);
      FileStatus[] fileStatusValues = readerFileSystem.listStatus(readerPath, new PathFilter()
      {
        @Override
        public boolean accept(Path path)
        {
          return path.getName().startsWith("part-");
        }

      });

      Text key = new Text();
      Text value = new Text();
      for (FileStatus fileStatus : fileStatusValues)
      {
        reader = new SequenceFile.Reader(readerFileSystem, fileStatus.getPath(), readerJobConfiguration);
        while (reader.next(key, value))
        {
          bufferedWriter.write(key.toString() + " = " + value.toString());
          bufferedWriter.newLine();
        }
        reader.close();
      }
    }
    catch (Exception ex)
    {
      System.out.printf(ex.getMessage());
      ToolRunner.printGenericCommandUsage(System.out);
    }
    finally
    {
      if (bufferedWriter != null)
      {
        try
        {
          bufferedWriter.close();
        }
        catch (Exception ex)
        {

        }
      }
      if (reader != null)
      {
        try
        {
          reader.close();
        }
        catch (Exception ex)
        {

        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception
  {

    if (args.length != 2)
    {
      System.out.printf("Usage: %s [generic options] <Matrix M> <Matrix N>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputFileMatrixM = args[0];
    inputFileMatrixN = args[1];
    inputPath = "input" + this.getClass().getName();
    outputPath = "output" + this.getClass().getName();

    JobConf inputFileDeleteJobConfiguration = new JobConf();
    Path inputFileDeletePath = new Path(inputPath + "/" + INPUT_FILE);
    FileSystem inputFileDeleteFileSystem = inputFileDeletePath.getFileSystem(inputFileDeleteJobConfiguration);
    inputFileDeleteFileSystem.delete(inputFileDeletePath, true);

    JobConf outputFileDeleteJobConfiguration = new JobConf();
    Path outputFileDeletePath = new Path(outputPath);
    FileSystem outputFileDeleteFileSystem = outputFileDeletePath.getFileSystem(outputFileDeleteJobConfiguration);
    outputFileDeleteFileSystem.delete(outputFileDeletePath, true);

    if (generateInputFile())
    {
      String currentInputPath = inputPath;
      String currentOutputPath = inputPath;
      for (int stepNumber = 0; stepNumber < 2; stepNumber++)
      {
        currentInputPath = currentOutputPath;
        currentOutputPath = outputPath + "/" + stepNumber;

        JobConf conf = new JobConf(getConf(), MatrixMultiplicationOneDriver.class);
        conf.setJobName(this.getClass().getName());
        conf.setInt(STEP_NUMBER, stepNumber);

        SequenceFileInputFormat.setInputPaths(conf, new Path(currentInputPath));
        conf.setInputFormat(SequenceFileInputFormat.class);

        SequenceFileOutputFormat.setOutputPath(conf, new Path(currentOutputPath));
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapperClass(MatrixMultiplicationOneMapper.class);
        if (stepNumber == 1)
        {
          conf.setOutputKeyComparatorClass(MatrixMultiplicationOneOutputKeyComparator.class);
        }
        conf.setReducerClass(MatrixMultiplicationOneReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        RunningJob job = JobClient.runJob(conf);
      }

      updateOutputFile(currentOutputPath);
    }

    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new MatrixMultiplicationOneDriver(), args);
    System.exit(exitCode);
  }
}
