import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AlgorithmMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
  private static final String DISCOVERED                 = "DISCOVERED";
  private static final String UNDISCOVERED               = "UNDISCOVERED";
  private static final String INFINITY                   = "INFINITY";
  private static final int    MINIMUM_SOURCE_NODE_TOKENS = 3;
  private static final int    MAXIMUM_SOURCE_NODE_TOKENS = 4;
  public static final int     INDEX_SOURCE_NODE_NUMBER   = 0;
  public static final int     INDEX_SOURCE_NODE_WEIGHT   = 1;
  private static final int    INDEX_SOURCE_NODE_STATUS   = 2;
  private static final int    INDEX_SOURCE_NODE_PATH     = 3;

  private Text                emptyText                  = new Text();

  @Override
  public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
  {
    String[] sourceNodeDetails = key.toString().substring(1, key.toString().length() - 1).split(",");
    if (sourceNodeDetails.length >= MINIMUM_SOURCE_NODE_TOKENS
        && !sourceNodeDetails[INDEX_SOURCE_NODE_WEIGHT].equalsIgnoreCase(INFINITY)
        && !sourceNodeDetails[INDEX_SOURCE_NODE_STATUS].equalsIgnoreCase(DISCOVERED))
    {
      String currentPath = "";
      if (sourceNodeDetails.length == MAXIMUM_SOURCE_NODE_TOKENS)
      {
        currentPath = sourceNodeDetails[INDEX_SOURCE_NODE_PATH];
      }
      currentPath += (currentPath.length() == 0 ? "" : "-") + sourceNodeDetails[INDEX_SOURCE_NODE_NUMBER];

      output.collect(new Text("{" + sourceNodeDetails[INDEX_SOURCE_NODE_NUMBER] + ","
          + sourceNodeDetails[INDEX_SOURCE_NODE_WEIGHT] + "," + DISCOVERED + "," + currentPath + "}"), value);

      if (value.toString().trim().length() > 0)
      {
        String[] tokens = value.toString().trim().split("\t");
        String[][] adjacentNodeDetails = new String[tokens.length][2];
        for (int index = 0; index < tokens.length; index++)
        {
          adjacentNodeDetails[index] = tokens[index].substring(1, tokens[index].length() - 1).split(",");
        }

        int sourceNodeWeight = Integer.parseInt(sourceNodeDetails[INDEX_SOURCE_NODE_WEIGHT]);
        for (int index = 0; index < tokens.length; index++)
        {
          int number = sourceNodeWeight + Integer.parseInt(adjacentNodeDetails[index][1]);

          output.collect(new Text("{" + adjacentNodeDetails[index][0] + "," + Integer.toString(number) + ","
              + UNDISCOVERED + "," + currentPath + "}"), emptyText);

          reporter.incrCounter(AlgorithmDriver.CUSTOM_COUNTERS,
              AlgorithmDriver.NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED, 1);
        }
      }
    }
    else
    {
      output.collect(key, value);
    }
  }
}
