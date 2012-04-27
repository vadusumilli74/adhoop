import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.CsvRecordInput;

public class OnTimeArrivalMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{

  public static final String    TITLE                          = ".....Time Period: Year / Month";
  public static final String    ON_TIME                        = ".......................On Time";
  public static final String    AIR_CARRIER_DELAY              = ".............Air Carrier Delay";
  public static final String    WEATHER_DELAY                  = ".................Weather Delay";
  public static final String    NATIONAL_AVIATION_SYSTEM_DELAY = "National Aviation System Delay";
  public static final String    SECURITY_DELAY                 = "................Security Delay";
  public static final String    AIRCRAFT_ARRIVING_LATE         = "........Aircraft Arriving Late";
  public static final String    CANCELLED                      = ".....................Cancelled";
  public static final String    DIVERTED                       = "......................Diverted";
  public static final String    TOTAL_OPERATIONS               = "..............Total Operations";
  public static final String    SEPARATOR                      = "#";

  private static final String   ONE                            = "1.0";
  private static final String[] DELAY_REASONS                  = { AIR_CARRIER_DELAY, WEATHER_DELAY,
      NATIONAL_AVIATION_SYSTEM_DELAY, SECURITY_DELAY, AIRCRAFT_ARRIVING_LATE };

  private Text naturalKey = new Text();
  private Text naturalValue = new Text();
  private boolean emitOnce = false;
  
  // Input Key: Line Number
  //
  // Input Value: One Record
  //
  // Output Key: <TITLE> + SEPARATOR + Year + SEPARATOR + Month  
  //
  // Output Value: Number + SEPARATOR + Year + SEPARATOR + Month
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
  {
    String [] tokens = value.toString().split("\\t");
    
    // Read Year & Month
    String year = tokens[0];
    String month = tokens[2];
    String period = year + SEPARATOR + month;

    if (year.equalsIgnoreCase("Year"))
    {
      emitOnce = true;
      return;
    }
    
    if (emitOnce)
    {
      // Sample Emit: .....Time Period: Year / Month#2011#10
      naturalKey.set(TITLE + SEPARATOR + period);
      naturalValue.set(period);
      output.collect(naturalKey, naturalValue);
      emitOnce = false;
    }
    // Sample Emit: ..............Total Operations#2011#10
    // Frequency: One / Key-Value Pair
    naturalKey.set(TOTAL_OPERATIONS + SEPARATOR + period);
    naturalValue.set(ONE + SEPARATOR + period);
    output.collect(naturalKey, naturalValue);
    
    // Sample Emit: .....................Cancelled#2011#10
    // Frequency: One / Key-Value Pair [If Cancelled]
    boolean isCancelled = false;
    try
    {
      isCancelled = (Integer.parseInt(tokens[47]) == 1);
    }
    catch(Exception e)
    {
      isCancelled = false;
    }
    if (isCancelled)
    {
      naturalKey.set(CANCELLED + SEPARATOR + period);
      naturalValue.set(ONE + SEPARATOR + period);
      output.collect(naturalKey, naturalValue);
      return;
    }
    
    // Sample Emit: ......................Diverted#2011#10
    // Frequency: One / Key-Value Pair [If Diverted]
    boolean isDiverted = false;
    try
    {
      isDiverted = (Integer.parseInt(tokens[49]) == 1);
    }
    catch(Exception e)
    {
      isDiverted = false;
    }
    if (isDiverted)
    {
      naturalKey.set(DIVERTED + SEPARATOR + period);
      naturalValue.set(ONE + SEPARATOR + period);
      output.collect(naturalKey, naturalValue);
      return;
    }
    
    // Sample Emit: .......................On Time#2011#10
    // Frequency: One / Key-Value Pair [If On time]
    double airDelayMinutes = 0.0;
    try
    {
      airDelayMinutes = Double.parseDouble(tokens[43]);
    }
    catch(Exception e)
    {
      airDelayMinutes = 0.0;
    }
    if (airDelayMinutes < 15.00)
    {
      naturalKey.set(ON_TIME + SEPARATOR + period);
      naturalValue.set(ONE + SEPARATOR + period);
      output.collect(naturalKey, naturalValue);
      return;
    }
    
    // Sample Emit: ................<Delay Reason>#2011#10
    // Frequency: One / Valid Every Delay Reason
    double [] airDelayMinutesSpecific = new double[5];
    double airDelayMinutesSpecificTotal = 0.0;
    for (int index = 0, indexToken = 56; index < 5; index++, indexToken++)
    {
      try
      {
        airDelayMinutesSpecific[index] = Double.parseDouble(tokens[indexToken]);
      }
      catch(Exception e)
      {
        airDelayMinutesSpecific[index] = 0.0;
      }
      
      airDelayMinutesSpecificTotal += airDelayMinutesSpecific[index];
    }
    for (int index = 0; index < 5; index++)
    {
      if (airDelayMinutesSpecific[index] > 0.0)
      {
        naturalKey.set(DELAY_REASONS[index] + SEPARATOR + period);
        naturalValue.set( Double.toString(airDelayMinutesSpecific[index] / airDelayMinutesSpecificTotal) + SEPARATOR + period);
        output.collect(naturalKey, naturalValue);
      }
    }
  }
}
