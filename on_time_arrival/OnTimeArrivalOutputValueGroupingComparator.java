import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OnTimeArrivalOutputValueGroupingComparator extends WritableComparator
{
  public OnTimeArrivalOutputValueGroupingComparator()
  {
    super(Text.class, true);
  }

  // Writable Comparable: <TITLE> + SEPARATOR + Year + SEPARATOR + Month
  // Group By: <TITLE>
  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {

    Text compositeKey1 = (Text) wc1;
    Text compositeKey2 = (Text) wc2;

    String[] tokens1 = compositeKey1.toString().split("#");
    String[] tokens2 = compositeKey2.toString().split("#");

    String naturalKey1 = tokens1[0];
    String naturalKey2 = tokens2[0];

    return naturalKey1.compareTo(naturalKey2);
  }
}