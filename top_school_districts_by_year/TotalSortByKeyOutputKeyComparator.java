import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TotalSortByKeyOutputKeyComparator extends WritableComparator
{

  public TotalSortByKeyOutputKeyComparator()
  {
    super(Text.class, true);
  }

  // Key: API
  // Sort: Decreasing Order
  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    Integer naturalKey1 = Integer.valueOf(wc1.toString());
    Integer naturalKey2 = Integer.valueOf(wc2.toString());

    return naturalKey1.compareTo(naturalKey2) * -1;
  }
}