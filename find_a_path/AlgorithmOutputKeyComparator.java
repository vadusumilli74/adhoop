import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AlgorithmOutputKeyComparator extends WritableComparator
{

  public AlgorithmOutputKeyComparator()
  {
    super(FromNodeKey.class, true);
  }

  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    FromNodeKey fromNodeKey1 = (FromNodeKey) wc1;
    FromNodeKey fromNodeKey2 = (FromNodeKey) wc2;
    
    Integer node1 = new Integer(fromNodeKey1.node());
    Integer node2 = new Integer(fromNodeKey2.node());
    Integer weight1 = new Integer(fromNodeKey1.nodeWeight());
    Integer weight2 = new Integer(fromNodeKey2.nodeWeight());

    int returnValue = node1.compareTo(node2);
    if (returnValue == 0)
    {
      returnValue = weight1.compareTo(weight2) * -1;
    }

    return returnValue;
  }
}