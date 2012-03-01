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
  //
  // Input Value: Rest of the Row Data
  //
  // Output Key: County/District Code # County/District Name
  //
  // Output Value: API # Number of Black or African American # Number of
  // American Indian or Alaska Native # Number of Asian # Number of Filipino #
  // Number of Hispanic or Latino # Number of Native Hawaiian or Pacific
  // Islander # Number of White # Average Parent Education Level # Enrolled for
  // Free or Reduced-Price Lunch
  @Override
  public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
  {

    String row = value.toString();
    String[] columns = row.split("\\t");

    if (columns[0].equalsIgnoreCase("S") && columns[1].equalsIgnoreCase(schoolType) && columns[2].trim().length() == 0
        && columns[3].trim().length() == 0 && columns[4].trim().length() == 0 && columns[8].trim().length() == 0)
    {
      String naturalKey = key.toString().substring(0, 7) + "#" + columns[6] + ", " + columns[7];
      StringBuilder naturalValue = new StringBuilder(columns[10] + "#");

      naturalValue.append(columns[17] + "#");
      naturalValue.append(columns[24] + "#");
      naturalValue.append(columns[31] + "#");
      naturalValue.append(columns[38] + "#");
      naturalValue.append(columns[45] + "#");
      naturalValue.append(columns[52] + "#");
      naturalValue.append(columns[59] + "#");
      if (year.equalsIgnoreCase("2011"))
      {
        naturalValue.append(columns[120] + "#");
        naturalValue.append(columns[102] + "#");
      }
      else if (year.equalsIgnoreCase("2010"))
      {
        naturalValue.append(columns[120] + "#");
        naturalValue.append(columns[102] + "#");
      }
      else if (year.equalsIgnoreCase("2009"))
      {
        naturalValue.append(columns[112] + "#");
        naturalValue.append(columns[94] + "#");
      }
      else if (year.equalsIgnoreCase("2008"))
      {
        naturalValue.append(columns[112] + "#");
        naturalValue.append(columns[94] + "#");
      }
      else if (year.equalsIgnoreCase("2007"))
      {
        naturalValue.append(columns[112] + "#");
        naturalValue.append(columns[94] + "#");
      }
      else if (year.equalsIgnoreCase("2006"))
      {
        naturalValue.append(columns[113] + "#");
        naturalValue.append(columns[94] + "#");
      }

      output.collect(new Text(naturalKey), new Text(naturalValue.toString()));
    }
  }
}
