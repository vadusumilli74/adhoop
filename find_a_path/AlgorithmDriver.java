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

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 
// INPUT:
//
// The input file is transformed into a Adjacency List and copied to the HDFS.
// 
// For every Source Node:
// Node = Node Number = RowIndex * ColumnCount + ColumnIndex
// Node Weight = 0 [For Starting Node]
//               Integer.MIN_VALUE [For Other Nodes]
// Discovered = False
// Path = Empty
//
// For every Destination Node
// Node = Node Number = RowIndex * ColumnCount + ColumnIndex
// Edge Weight = 1 [Per Requirement]
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 
// MAPPER:
// 
// Key: Source Node
// Value: Destination Nodes
//
// For every UNDISCOVERED Source Node with a VALID Weight, 
//  a. emits the Key-Value (Source Node - Destination Nodes) Pair with 
//     the Source Node marked DISCOVERED, and 
//     the Path updated by appending the Current Source Node Path with the Source Node
//  b. For each Destination Node, emits the Key-Value (Destination Node - Empty) Pair with 
//     the Destination Node marked UNDISCOVERED, and
//     the Path updated with the new Source Node Path updated in the previous step
//     the Node Weight = Source Node Weight + Destination Edge Weight
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// OUTPUT KEY & VALUE GROUPING COMPARATOR:
// 
// The OutputKeyComparator sorts the Keys by Node Number and Node Weight (in the Reverse Order).
// The OutputValueGroupingComparator groups the Keys by Node Number.
// This will automatically make the Source Node with the Maximum Node Weight (Edges) to be the Key in the Key-Value Pairs recieved by the Reducer. 
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// PARTITIONER:
//
// The Partitioner sends the records with the same Node Number to the same Reducer
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// REDUCER:
//
// The Reducer emits a Key-Value Pair using the recieved Key and a consolidated Value.
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//
// The Mapper/Reducer is run until no new Nodes are left UNDISCOVERED.
// This will provide all the Paths with the Maximum Weight (or Edges) from the Source to all possible Destinations.
// The above is repeated for all Source Nodes that have a Path to Destination Node.
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
public class AlgorithmDriver extends Configured implements Tool
{
  public static final String CUSTOM_COUNTERS          = "Custom Counters";
  public static final String NODE_NUMBER              = "NODE_NUMBER";
  public static final String JOB_NUMBER               = "JOB_NUMBER";
  public static final String UNDISCOVERED_NODES_COUNT = "UNDISCOVERED_NODES_COUNT";

  String                     inputFile                = "";
  String                     inputPath                = "";
  String                     outputFile               = "";
  String                     outputPath               = "";
  int                        countCellRows            = 0;
  int                        countCellColumns         = 0;
  int                        genomeLength             = 0;
  boolean[][]                matrix                   = null;
  BufferedWriter             bufferedWriter           = null;
  boolean                    foundAtLeatOnePath       = false;

  // Generate the Adjacency Matrix
  private boolean generateAdjacencyMatrix()
  {
    boolean returnValue = false;
    String errorMessage = "";

    BufferedReader bufferedReader = null;
    try
    {
      errorMessage = "Invalid Local Input File.";
      bufferedReader = new BufferedReader(new FileReader(new File(inputFile)));
      String line = bufferedReader.readLine();
      if (line != null)
      {
        String[] headerRowValues = line.split(" ");

        errorMessage = "Invalid Local Input File:: Header Data.";
        countCellRows = Integer.parseInt(headerRowValues[0]);
        countCellColumns = Integer.parseInt(headerRowValues[1]);
        genomeLength = Integer.parseInt(headerRowValues[2]);

        int row = 0;
        matrix = new boolean[countCellRows][countCellColumns];
        while ((line = bufferedReader.readLine()) != null)
        {
          if (row < countCellRows)
          {
            line = line.trim();
            for (int column = 0; column < countCellColumns; column++)
            {
              if (column < line.length())
              {
                matrix[row][column] = (line.charAt(column) == 'o');
              }
              else
              {
                matrix[row][column] = false;
              }
            }
            row++;
          }
          else
          {
            System.out.printf("Invalid Local Input File:: Ignoring Excess Row Data.");
            ToolRunner.printGenericCommandUsage(System.out);
            break;
          }
        }

        returnValue = true;
      }
    }
    catch (Exception ex)
    {
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

      if (!returnValue)
      {
        System.out.printf(errorMessage);
        ToolRunner.printGenericCommandUsage(System.out);
      }
    }

    return returnValue;
  }

  // Generate the Adjacency List using the Adjacency Matrix ad mark the Source
  // Node
  private boolean generateInputFile(int startingNode)
  {
    boolean returnValue = false;
    String errorMessage = "";

    Writer writer = null;
    try
    {
      Path writerPath = new Path(inputPath + "/" + inputFile);
      JobConf writerJobConfiguration = new JobConf();
      FileSystem writerFileSystem = writerPath.getFileSystem(writerJobConfiguration);
      writer = SequenceFile.createWriter(writerFileSystem, writerJobConfiguration, writerPath, FromNodeKey.class,
          ToNodeValues.class);
      int newRow = 0;
      int newColumn = 0;
      for (int row = 0; row < countCellRows; row++)
      {
        for (int column = 0; column < countCellColumns; column++)
        {
          if (matrix[row][column])
          {
            FromNodeKey key = new FromNodeKey();
            key.node(row * countCellColumns + column);
            key.nodeWeight(key.node() == startingNode ? 0 : Integer.MIN_VALUE);
            key.discovered(false);
            ToNodeValues value = new ToNodeValues();
            for (int offsetRow = -1; offsetRow <= 1; offsetRow++)
            {
              for (int offsetColumn = -1; offsetColumn <= 1; offsetColumn++)
              {
                newRow = row + offsetRow;
                newColumn = column + offsetColumn;
                if ((offsetRow == 0 || offsetColumn == 0) && !(offsetRow == 0 && offsetColumn == 0))
                {
                  if (newRow >= 0 && newRow < countCellRows && newColumn >= 0 && newColumn < countCellColumns
                      && matrix[newRow][newColumn])
                  {
                    ToNodeValue toNodeValue = new ToNodeValue();
                    toNodeValue.node(newRow * countCellColumns + newColumn);
                    toNodeValue.edgeWeight(1);

                    value.add(toNodeValue);
                  }
                }
              }
            }
            // Ignore if there are no Paths
            if (value.size() > 0)
            {
              writer.append(key, value);
            }
          }
        }
      }
      returnValue = true;
    }
    catch (Exception ex)
    {
    }
    finally
    {
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

      if (!returnValue)
      {
        System.out.printf(errorMessage);
        ToolRunner.printGenericCommandUsage(System.out);
      }
    }

    return returnValue;
  }

  // Update the Output File if any Paths with the requested Weight are found.
  private void updateOutputFile(String currentOutputPath)
  {
    SequenceFile.Reader reader = null;
    try
    {
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

      FromNodeKey key = new FromNodeKey();
      ToNodeValues value = new ToNodeValues();
      for (FileStatus fileStatus : fileStatusValues)
      {
        reader = new SequenceFile.Reader(readerFileSystem, fileStatus.getPath(), readerJobConfiguration);
        while (reader.next(key, value))
        {
          String[] nodes = key.path().split("-");
          if (nodes.length == genomeLength)
          {
            foundAtLeatOnePath = true;
            
            bufferedWriter.write("Path = ");
            for (int index = 0; index < nodes.length; index++)
            {
              int node = Integer.parseInt(nodes[index]);
              bufferedWriter.write("(" + (node / countCellColumns) + "," + (node % countCellColumns) + ") ");
            }
            bufferedWriter.newLine();
          }
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

    if (args.length != 4)
    {
      System.out
          .printf(
              "Usage: %s [generic options] <Local Input File> <Local Output File> <HDFS Input Directory> <HDFS Output Directory>\n",
              getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    inputFile = args[0];
    inputPath = args[2];
    outputFile = args[1];
    outputPath = args[3];

    JobConf outputFileDeleteJobConfiguration = new JobConf();
    Path outputFileDeletePath = new Path(outputFile);
    FileSystem outputFileDeleteFileSystem = outputFileDeletePath.getFileSystem(outputFileDeleteJobConfiguration);
    outputFileDeleteFileSystem.delete(outputFileDeletePath, false);

    if (!generateAdjacencyMatrix())
    {
      return 0;
    }

    try
    {
      bufferedWriter = new BufferedWriter(new FileWriter(new File(outputFile)));
    }
    catch (Exception ex)
    {
      System.out.printf(ex.getMessage());
      ToolRunner.printGenericCommandUsage(System.out);
      return 0;
    }

    for (int row = 0; row < countCellRows; row++)
    {
      for (int column = 0; column < countCellColumns; column++)
      {
        // For every Node or Vacant Cell
        if (matrix[row][column])
        {
          JobConf inputPathDeleteJobConfiguration = new JobConf();
          Path inputPathDeletePath = new Path(inputPath);
          FileSystem inputPathDeleteFileSystem = inputPathDeletePath.getFileSystem(inputPathDeleteJobConfiguration);
          inputPathDeleteFileSystem.delete(inputPathDeletePath, true);

          JobConf outputPathDeleteJobConfiguration = new JobConf();
          Path outputPathDeletePath = new Path(outputPath);
          FileSystem outputPathDeleteFileSystem = outputPathDeletePath.getFileSystem(outputPathDeleteJobConfiguration);
          outputPathDeleteFileSystem.delete(outputPathDeletePath, true);

          // Run the MapReduce Job until all possible Nodes or Vacant Cells are
          // Discovered.
          if (generateInputFile(row * countCellColumns + column))
          {
            RunningJob job = null;
            int iteration = 0;
            String currentInputPath = outputPath;
            String currentOutputPath = inputPath;
            do
            {
              currentInputPath = currentOutputPath;
              currentOutputPath = outputPath + "/" + iteration;

              JobConf conf = new JobConf(getConf(), AlgorithmDriver.class);
              conf.setJobName(this.getClass().getName());
              conf.setInt(JOB_NUMBER, iteration + 1);
              conf.setInt(NODE_NUMBER, row * countCellColumns + column);

              // Initial Input Path = User Provided Input Path
              // Intermediate Output Path = User Provided Output Path/Current
              // Iteration
              // Intermediate Input Path = Previous Intermediate Output Path
              // Final Output Path = User Provided Output Path/Final
              SequenceFileInputFormat.setInputPaths(conf, new Path(currentInputPath));
              conf.setInputFormat(SequenceFileInputFormat.class);

              SequenceFileOutputFormat.setOutputPath(conf, new Path(currentOutputPath));
              conf.setOutputFormat(SequenceFileOutputFormat.class);

              conf.setMapperClass(AlgorithmMapper.class);
              conf.setOutputKeyComparatorClass(AlgorithmOutputKeyComparator.class);
              conf.setOutputValueGroupingComparator(AlgorithmOutputValueGroupingComparator.class);
              conf.setPartitionerClass(AlgorithmPartitioner.class);
              conf.setReducerClass(AlgorithmReducer.class);

              conf.setMapOutputKeyClass(FromNodeKey.class);
              conf.setMapOutputValueClass(ToNodeValues.class);

              conf.setOutputKeyClass(FromNodeKey.class);
              conf.setOutputValueClass(ToNodeValues.class);

              job = JobClient.runJob(conf);

              iteration++;
            }
            while (job.getCounters().findCounter(CUSTOM_COUNTERS, UNDISCOVERED_NODES_COUNT).getValue() > 0);

            updateOutputFile(currentOutputPath);
          }
        }
      }
    }

    if (bufferedWriter != null)
    {
      try
      {
        if (!foundAtLeatOnePath)
        {
          bufferedWriter.write("Impossible");
        }
        bufferedWriter.close();
      }
      catch (Exception ex)
      {

      }
    }

    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new AlgorithmDriver(), args);
    System.exit(exitCode);
  }
}
