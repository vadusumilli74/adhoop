import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TopSchoolsMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
  private String[] HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY = { "Black or African American",
      "American Indian or Alaska Native", "Asian", "Filipino", "Hispanic or Latino",
      "Native Hawaiian or Pacific Islander", "White"                   };

  private String   schoolType;
  private String   year;

  @Override
  public void configure(JobConf job)
  {
    schoolType = job.get(TopSchoolsDriver.SCHOOL_TYPE);
    year = job.get(TopSchoolsDriver.YEAR);
  }

  // Input Key: CDS = County/District/School Code
  // Input Value: Rest of the Row Data
  // Output Key: API = Academic Performance Index
  // Output Value: Highest % of Students by Race and Ethnicity #
  // Average Parent Education Level #
  // Enrolled for Free or Reduced-Price Lunch
  @Override
  public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
  {

    String row = value.toString();
    String[] columns = row.split("\\t");

    if (columns[0].equalsIgnoreCase("S") && columns[1].equalsIgnoreCase(schoolType) && columns[2].trim().length() == 0
        && columns[3].trim().length() == 0 && columns[4].trim().length() == 0 && columns[8].trim().length() == 0)
    {
      String naturalKey = columns[10];
      String naturalValue = "";

      if (year.equals("2011"))
      {
        naturalValue += percentageRace(columns[94], columns[95], columns[96], columns[97], columns[98], columns[99],
            columns[100]) + "#";
        naturalValue += columns[120] + "#";
        naturalValue += columns[102] + "#";
      }
      else if (year.equalsIgnoreCase("2010"))
      {
        naturalValue += percentageRace(columns[94], columns[95], columns[96], columns[97], columns[98], columns[99],
            columns[100]) + "#";
        naturalValue += columns[120] + "#";
        naturalValue += columns[102] + "#";
      }
      else if (year.equalsIgnoreCase("2009"))
      {
        naturalValue += percentageRace(columns[87], columns[88], columns[89], columns[90], columns[91], columns[92],
            columns[93]) + "#";
        naturalValue += columns[112] + "#";
        naturalValue += columns[94] + "#";
      }
      else if (year.equalsIgnoreCase("2008"))
      {
        naturalValue += percentageRace(columns[87], columns[88], columns[89], columns[90], columns[91], columns[92],
            columns[93]) + "#";
        naturalValue += columns[112] + "#";
        naturalValue += columns[94] + "#";
      }
      else if (year.equalsIgnoreCase("2007"))
      {
        naturalValue += percentageRace(columns[87], columns[88], columns[89], columns[90], columns[91], columns[92],
            columns[93]) + "#";
        naturalValue += columns[112] + "#";
        naturalValue += columns[94] + "#";
      }
      else if (year.equalsIgnoreCase("2006"))
      {
        naturalValue += percentageRace(columns[87], columns[88], columns[89], columns[90], columns[91], columns[92],
            columns[93]) + "#";
        naturalValue += columns[113] + "#";
        naturalValue += columns[94] + "#";
      }
      naturalValue += columns[5] + ", " + columns[6] + ", " + columns[7];
      output.collect(new Text(naturalKey), new Text(naturalValue));
    }
  }

  public String percentageRace(String... valueStrings)
  {
    int index = 0;
    int maximumIndicies = HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY.length;
    int maximumValue = -1;
    String message = "";
    int[] values = new int[valueStrings.length];

    for (String valueString : valueStrings)
    {
      try
      {
        values[index] = Integer.parseInt(valueString);
      }
      catch (Exception e)
      {
        values[index] = 0;
      }

      if (maximumValue == -1)
      {
        maximumValue = values[index];
        message = index < maximumIndicies ? HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY[index] : "Unknown";
      }
      else if (maximumValue < values[index])
      {
        maximumValue = values[index];
        message = index < maximumIndicies ? HIGHEST_PERCENTAGE_OF_STUDENTS_BY_RACE_AND_ETHNICITY[index] : "Unknown";
      }
      index++;
    }

    message += " (" + maximumValue + "%)";

    return message;
  }
}
