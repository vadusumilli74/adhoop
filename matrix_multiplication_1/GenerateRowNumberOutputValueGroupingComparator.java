import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GenerateRowNumberOutputValueGroupingComparator extends WritableComparator
{
  public GenerateRowNumberOutputValueGroupingComparator()
  {
    super(Text.class, true);
  }

  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    String [] wcTokens1 = wc1.toString().split(", ");
    String [] wcTokens2 = wc2.toString().split(", ");
    
    int returnValue = wcTokens1[0].compareTo(wcTokens2[0]);

    return returnValue;
  }
}