import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopSchoolsOutputValueGroupingComparator extends WritableComparator
{
  public TopSchoolsOutputValueGroupingComparator()
  {
    super(Text.class, true);
  }

  // Input Key: County/District Code # County/District Name
  // Sorting Key: County/District Code
  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    String naturalKey1 = wc1.toString().split("#")[0];
    String naturalKey2 = wc2.toString().split("#")[0];

    return naturalKey1.compareTo(naturalKey2);
  }
}