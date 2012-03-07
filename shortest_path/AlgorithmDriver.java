import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Reference: Data-Intensive Text Processing with MapReduce - Jimmy Lin and Chris Dyer
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Distance to each node is initialized to INFINITY for all nodes except for the source node. 
// 
// For every UNDISCOVERED with a VALID Weight, the Mapper 
//  a. emits the Key-Value Pair with the Node marked DISCOVERED
//  b. works by mapping over all nodes and emitting a key-value pair for each neighbor on the node’s adjacency list.
//
// Key = {Neighbor Node Number, Source Weight + Edge Weight, UNDISCOVERED, Shortest Path to Source Node}
//
// The OutputKeyComparator sorts the Keys by Node Number and Source Weight + Edge Weight
// The OutputValueGroupingComparator groups the Keys by Node Number
// The Partitioner sends the records with the same Node Number to the same Reducer
//
// The above allows the Reducer to pick the Key with the minimum Weight along with its adjacency list
//
// The Mapper/Reducer is run until none of the Nodes are UNDISCOVERED
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Initial Input File Format:
//    First Record: {Source Node Number, 0, UNDISCOVERED} <TAB> <Destination Node Number, Edge Weight> <TAB> <Destination Node Number, Edge Weight> ...
//    Example: {1,0,UNDISCOVERED}  {2,40}  {3,8} {4,10}
//
//    Other Records: {Source Node Number, INFINITY, UNDISCOVERED} <TAB> <Destination Node Number, Edge Weight> <TAB> <Destination Node Number, Edge Weight> ...
//    Example: {2,INFINITY,UNDISCOVERED} {5,6} {7,10}
// 
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//    Sample Input File:
//    {1,0, UNDISCOVERED} {2,40}  {3,8} {4,10}
//    {2,INFINITY, UNDISCOVERED}  {5,6} {7,10}
//    {3,INFINITY, UNDISCOVERED}  {2,4} {4,12}  {6,2}
//    {4,INFINITY, UNDISCOVERED}  {6,1}
//    {5,INFINITY, UNDISCOVERED}  {3,2} {6,2} {7,4}
//    {6,INFINITY, UNDISCOVERED}  {8,4} {9,3}
//    {7,INFINITY, UNDISCOVERED}  {8,20}  {10,1}
//    {8,INFINITY, UNDISCOVERED}  {5,0} {10,20}
//    {9,INFINITY, UNDISCOVERED}  {4,6} {10,2}
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Intermediate / Final Output File Format:
//    All Records: {Source Node Number, Minimum Edge Weight, DISCOVERED, Path Taken} <TAB> <Destination Node Number, Edge Weight> <TAB> <Destination Node Number, Edge Weight> ...
//    Example: {9,13, DISCOVERED,1-3-6}    {4,6}   {10,2}
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Sample Output File:
// {1,0,DISCOVERED,1}      {2,40}  {3,8}   {4,10}
// {2,12,DISCOVERED,1-3-2} {5,6}   {7,10}
// {3,8,DISCOVERED,1-3}    {2,4}   {4,12}  {6,2}
// {4,10,DISCOVERED,1-4}   {6,1}
// {5,14,DISCOVERED,1-3-6-8-5}     {3,2}   {6,2}   {7,4}
// {6,10,DISCOVERED,1-3-6} {8,4}   {9,3}
// {7,18,DISCOVERED,1-3-6-8-5-7}   {8,20}  {10,1}
// {8,14,DISCOVERED,1-3-6-8}       {5,0}   {10,20}
// {9,13,DISCOVERED,1-3-6-9}       {4,6}   {10,2}
// {10,15,DISCOVERED,1-3-6-9-10}
//
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
public class AlgorithmDriver extends Configured implements Tool
{
  public static final String CUSTOM_COUNTERS                              = "Custom Counters";
  public static final String NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED = "NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED";

  @Override
  public int run(String[] args) throws Exception
  {

    if (args.length != 2)
    {
      System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    int iteration = 0;
    long toBeProcessed = 0;
    boolean finalRun = false;
    String inputPath = args[0];
    String outputPath = args[1];
    do
    {
      JobConf conf = new JobConf(getConf(), AlgorithmDriver.class);
      conf.setJobName(this.getClass().getName());

      // Initial Input Path = User Provided Input Path
      // Intermediate Output Path = User Provided Output Path - Number
      // Intermediate Input Path = Previous Output Path
      // Final Output Path = User Provided Output Path
      FileInputFormat.setInputPaths(conf, new Path(inputPath));
      FileOutputFormat.setOutputPath(conf, new Path(finalRun ? outputPath : outputPath + "-" + iteration));

      conf.setInputFormat(KeyValueTextInputFormat.class);

      conf.setMapperClass(AlgorithmMapper.class);
      conf.setOutputKeyComparatorClass(AlgorithmOutputKeyComparator.class);
      conf.setOutputValueGroupingComparator(AlgorithmOutputValueGroupingComparator.class);
      conf.setPartitionerClass(AlgorithmPartitioner.class);
      conf.setReducerClass(AlgorithmReducer.class);

      // Set Final Run Number of Reduce Tasks to 1 to sort the output
      if (finalRun)
      {
        conf.setNumReduceTasks(1);
      }

      conf.setMapOutputKeyClass(Text.class);
      conf.setMapOutputValueClass(Text.class);

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      RunningJob job = JobClient.runJob(conf);

      inputPath = outputPath + "-" + iteration;
      iteration++;
      toBeProcessed = job.getCounters().findCounter(CUSTOM_COUNTERS, NUMBER_OF_UNDISCOVERED_NODES_TO_BE_PROCESSED)
          .getValue();
      // Run one more time to facilitate Sorting and Output to the User Provided Folder
      finalRun = (toBeProcessed == 0 && !finalRun);
    }
    while (toBeProcessed > 0 || finalRun);

    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new AlgorithmDriver(), args);
    System.exit(exitCode);
  }
}
