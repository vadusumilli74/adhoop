import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AnagramSortedValuesOutputValueGroupingComparator extends WritableComparator
{

  public AnagramSortedValuesOutputValueGroupingComparator()
  {
    super(Text.class, true);
  }

  // Key: Sorted Characters of Word # Word
  // Grouping: Sorted Characters of Word
  // This will ensure all the words that are anagrams of each other are grouped together
  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2)
  {
    Text compositeKey1 = (Text) wc1;
    Text compositeKey2 = (Text) wc2;

    String naturalKey1 = compositeKey1.toString().split("#")[0];
    String naturalKey2 = compositeKey2.toString().split("#")[0];

    return naturalKey1.compareTo(naturalKey2);
  }
}