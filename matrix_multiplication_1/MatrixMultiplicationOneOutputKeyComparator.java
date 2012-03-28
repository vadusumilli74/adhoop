import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MatrixMultiplicationOneOutputKeyComparator extends WritableComparator
{

  public MatrixMultiplicationOneOutputKeyComparator()
  {
    super(Text.class, true);
  }

  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    String [] tokens1 = wc1.toString().split(MatrixMultiplicationOneDriver.SEPARATOR);
    String [] tokens2 = wc2.toString().split(MatrixMultiplicationOneDriver.SEPARATOR);
    
    Integer row1 = new Integer(tokens1[0]);
    Integer row2 = new Integer(tokens2[0]);
    Integer column1 = new Integer(tokens1[1]);
    Integer column2 = new Integer(tokens2[1]);

    int returnValue = row1.compareTo(row2);
    if (returnValue == 0)
    {
      returnValue = column1.compareTo(column2);
    }

    return returnValue;
  }
}