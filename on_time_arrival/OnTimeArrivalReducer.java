import java.io.IOException;
import java.text.DateFormatSymbols;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class OnTimeArrivalReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{

  private class YearCount
  {
    private String mYear  = "";
    private Double mCount = 0.0;

    public void year(String value)
    {
      mYear = value;
    }

    public String year()
    {
      return mYear;
    }

    public void incrementCount(double value)
    {
      mCount += value;
    }

    public Double count()
    {
      return mCount;
    }

    @Override
    public String toString()
    {
      return mYear;
    }
  }

  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
  {

    Hashtable<String, YearCount> years = new Hashtable<String, YearCount>();
    Text naturalKey = new Text(key.toString().split("#")[0]);
    String[] tokens;
    String value = "";
    String naturalValue = "";
    String month = "";
    String year = "";

    if (key.toString().startsWith(OnTimeArrivalMapper.TITLE))
    {
      String[] months = (new DateFormatSymbols()).getMonths();
      while (values.hasNext())
      {
        value = values.next().toString();
        tokens = value.split("#");
        year = tokens[0];
        month = tokens[1];

        if (!years.containsKey(value))
        {
          years.put(value, new YearCount());
          naturalValue += year + " / " + month + "\t";
        }
      }
      output.collect(naturalKey, new Text(naturalValue));
    }
    else
    {
      Vector<String> yearsOrdered = new Vector<String>();
      String count = "";
      while (values.hasNext())
      {
        value = values.next().toString();
        tokens = value.split("#");
        count = tokens[0];
        year = tokens[1];
        month = tokens[2];

        if (!years.containsKey(year + month))
        {
          years.put(year + month, new YearCount());
          yearsOrdered.add(yearsOrdered.size(), year + month);
        }
        ((YearCount) years.get(year + month)).incrementCount(Double.valueOf(count));
      }

      value = "";
      for (int index = 0; index < yearsOrdered.size(); index++)
      {
        value += String.valueOf(((YearCount) years.get(yearsOrdered.get(index))).count().intValue()) + "\t\t";
      }
      output.collect(naturalKey, new Text(value));
    }
  }
}
