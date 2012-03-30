import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GenerateRowNumberOutputKeyComparator extends WritableComparator
{

  public GenerateRowNumberOutputKeyComparator()
  {
    super(Text.class, true);
  }

  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    String [] wcTokens1 = wc1.toString().split(", ");
    String [] wcTokens2 = wc2.toString().split(", ");
    
    Long byteOffset1 = new Long(wcTokens1[1]);
    Long byteOffset2 = new Long(wcTokens2[1]);
    Long lineNumber1 = new Long(wcTokens1[2]);
    Long lineNumber2 = new Long(wcTokens2[2]);

    int returnValue = wcTokens1[0].compareTo(wcTokens2[0]);
    if (returnValue == 0)
    {
      returnValue = byteOffset1.compareTo(byteOffset2);
      if (returnValue == 0)
      {
        returnValue = lineNumber1.compareTo(lineNumber2);
      }
    }

    return returnValue;
  }
}