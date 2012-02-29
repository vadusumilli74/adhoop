import java.net.URI;

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

public class TopSchoolsDriver extends Configured implements Tool
{
  public static final int    MAXIMUM_REDUCERS = 4;
  public static final int    MAXIMUM_API      = 1000;
  public static final int    API_BLOCK_SIZE   = MAXIMUM_API / MAXIMUM_REDUCERS;
  public static final String SCHOOL_TYPE      = "School Type";
  public static final String YEAR             = "Year";

  @Override
  public int run(String[] args) throws Exception
  {

    if (args.length != 4)
    {
      System.out.printf("Usage: %s [generic options] <input dir> <output dir> <YEAR> <SCHOOL_TYPE = E | M | H>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    JobConf conf = new JobConf(getConf(), TopSchoolsDriver.class);
    conf.setJobName(this.getClass().getName());

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.set(SCHOOL_TYPE, args[3]);
    conf.set(YEAR, args[2]);
    conf.setInputFormat(KeyValueTextInputFormat.class);

    conf.setMapperClass(TopSchoolsMapper.class);
    conf.setOutputKeyComparatorClass(TopSchoolsOutputKeyComparator.class);
    conf.setPartitionerClass(TopSchoolsPartitioner.class);
    conf.setReducerClass(TopSchoolsReducer.class);

    conf.setNumReduceTasks(MAXIMUM_REDUCERS);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    JobClient.runJob(conf);

    return 0;
  }

  public static void main(String[] args) throws Exception
  {
    int exitCode = ToolRunner.run(new TopSchoolsDriver(), args);
    System.exit(exitCode);
  }
}
