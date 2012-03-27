import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AlgorithmOutputValueGroupingComparator extends WritableComparator
{
  public AlgorithmOutputValueGroupingComparator()
  {
    super(Text.class, true);
  }

  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    FromNodeKey fromNodeKey1 = (FromNodeKey) wc1;
    FromNodeKey fromNodeKey2 = (FromNodeKey) wc2;
    
    Integer node1 = new Integer(fromNodeKey1.node());
    Integer node2 = new Integer(fromNodeKey2.node());
    
    int returnValue = node1.compareTo(node2);

    return returnValue;
  }
}