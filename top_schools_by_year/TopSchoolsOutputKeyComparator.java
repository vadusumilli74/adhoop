import java.util.Hashtable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopSchoolsOutputKeyComparator extends WritableComparator
{

  public TopSchoolsOutputKeyComparator()
  {
    super(Text.class, true);
  }

  // Key: API
  // Sort: Decreasing Order
  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    Integer api1 = Integer.valueOf(wc1.toString());
    Integer api2 = Integer.valueOf(wc2.toString());

    return api1.compareTo(api2) * -1;
  }
}