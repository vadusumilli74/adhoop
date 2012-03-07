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
    String compositeKey1 = ((Text) wc1).toString().trim();
    String compositeKey2 = ((Text) wc2).toString().trim();

    String[] nodeDetails1 = compositeKey1.substring(1, compositeKey1.length() - 1).split(",");
    String[] nodeDetails2 = compositeKey2.substring(1, compositeKey2.length() - 1).split(",");

    Integer node1 = null;
    Integer node2 = null;
    try
    {
      node1 = Integer.valueOf(nodeDetails1[AlgorithmMapper.INDEX_SOURCE_NODE_NUMBER]);
    }
    catch (Exception e)
    {
      node1 = new Integer(Integer.MAX_VALUE);
    }
    try
    {
      node2 = Integer.valueOf(nodeDetails2[AlgorithmMapper.INDEX_SOURCE_NODE_NUMBER]);
    }
    catch (Exception e)
    {
      node2 = new Integer(Integer.MAX_VALUE);
    }

    int returnValue = node1.compareTo(node2);

    return returnValue;
  }
}