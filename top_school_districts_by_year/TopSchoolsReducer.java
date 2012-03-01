import java.io.IOException;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TopSchoolsReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
  private String[] HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY = { "Black or African American",
      "American Indian or Alaska Native", "Asian", "Filipino", "Hispanic or Latino",
      "Native Hawaiian or Pacific Islander", "White"                   };

  private String schoolType;
  private String year;

  @Override
  public void configure(JobConf job)
  {
    schoolType = job.get(TopSchoolsDriver.SCHOOL_TYPE);
    year = job.get(TopSchoolsDriver.YEAR);
  }

  // Input Key: County/District Code # County/District Name
  //
  // Input Value: API # Number of Black or African American # Number of
  // American Indian or Alaska Native # Number of Asian # Number of Filipino #
  // Number of Hispanic or Latino # Number of Native Hawaiian or Pacific
  // Islander # Number of White # Average Parent Education Level # Enrolled for
  // Free or Reduced-Price Lunch
  //
  // Output Key: API
  //
  // Output Value: Number of Black or African American # Number of
  // American Indian or Alaska Native # Number of Asian # Number of Filipino #
  // Number of Hispanic or Latino # Number of Native Hawaiian or Pacific
  // Islander # Number of White # Average Parent Education Level # Enrolled for
  // Free or Reduced-Price Lunch # County/District Name
  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {

    double[] tokenValues = null;
    double count = 0.0;
    double totalStudents = 0.0;
    double value = 0.0;
    while (values.hasNext())
    {
      String[] tokens = values.next().toString().split("#");
      if (tokenValues == null)
      {
        tokenValues = new double[tokens.length];
        for (int index = 0; index < tokens.length; index++)
        {
          tokenValues[index] = 0.0;
        }
      }
      
      for (int index = 0; index < tokens.length; index++)
      {
        try
        {
          value = Double.parseDouble(tokens[index]);
        }
        catch (NumberFormatException e)
        {
          value = 0.0;
        }
        
        tokenValues[index] += value;
        
        if (index > 0 && index < 8)
        {
          totalStudents += value;
        }
      }
      
      count += 1.0;
    }

    if ((schoolType.equalsIgnoreCase("E") && count >= 5.0) || (schoolType.equalsIgnoreCase("M") && count >= 2.0)
        || (schoolType.equalsIgnoreCase("H") && count >= 2.0))
    {
      for (int index = 0; index < tokenValues.length; index++)
      {
        if (index > 0 && index < 8)
        {
          tokenValues[index] = ((tokenValues[index] * 100.00) / totalStudents);
        }
        else if (index != 1)
        {
          tokenValues[index] /= count;
        }
      }

      String naturalKey = Integer.toString((int) tokenValues[0]);
      String naturalValue = percentageRace(tokenValues[1], tokenValues[2], tokenValues[3], tokenValues[4],
          tokenValues[5], tokenValues[6], tokenValues[7])
          + "|"
          + (new DecimalFormat("0.00")).format(tokenValues[8])
          + "|" + Integer.toString((int) tokenValues[9]) + "|" + key.toString().split("#")[1];
      output.collect(new Text(naturalKey), new Text(naturalValue));
    }
  }

  public String percentageRace(double... values)
  {
    int index = 0;
    int maximumIndicies = HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY.length;
    double maximumValue = -1.0;
    String message = "";

    for (double value : values)
    {
      if (maximumValue == -1)
      {
        maximumValue = value;
        message = index < maximumIndicies ? HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY[index] : "Unknown";
      }
      else if (maximumValue < value)
      {
        maximumValue = value;
        message = index < maximumIndicies ? HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY[index] : "Unknown";
      }
      index++;
    }

    message += " (" + (new DecimalFormat("0")).format(maximumValue) + "%)";

    return message;
  }
}
