import java.util.Hashtable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OnTimeArrivalOutputKeyComparator extends WritableComparator {
	private Hashtable<String, Integer> mSortingOrder;
	
	public OnTimeArrivalOutputKeyComparator() {
		super(Text.class, true);
		
		mSortingOrder = new Hashtable<String, Integer>();
		mSortingOrder.put(OnTimeArrivalMapper.TITLE, new Integer(-1));
		mSortingOrder.put(OnTimeArrivalMapper.ON_TIME, new Integer(0));
		mSortingOrder.put(OnTimeArrivalMapper.AIR_CARRIER_DELAY, new Integer(1));
		mSortingOrder.put(OnTimeArrivalMapper.WEATHER_DELAY, new Integer(2));
		mSortingOrder.put(OnTimeArrivalMapper.NATIONAL_AVIATION_SYSTEM_DELAY, new Integer(3));
		mSortingOrder.put(OnTimeArrivalMapper.SECURITY_DELAY, new Integer(4));
		mSortingOrder.put(OnTimeArrivalMapper.AIRCRAFT_ARRIVING_LATE, new Integer(5));
		mSortingOrder.put(OnTimeArrivalMapper.CANCELLED, new Integer(6));
		mSortingOrder.put(OnTimeArrivalMapper.DIVERTED, new Integer(7));
		mSortingOrder.put(OnTimeArrivalMapper.TOTAL_OPERATIONS, new Integer(8));
	}

  // Writable Comparable: <TITLE> + SEPARATOR + Year + SEPARATOR + Month
	// Sort By: <TITLE> [Specific Order], Year [Descending Order], Month [Descending Order]
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		Text compositeKey1 = (Text) wc1;
		Text compositeKey2 = (Text) wc2;
		
		String [] tokens1 = compositeKey1.toString().split("#");
		String [] tokens2 = compositeKey2.toString().split("#");
		
		String naturalKey1 = tokens1[0];
		String naturalKey2 = tokens2[0];
		
		Integer year1 = Integer.valueOf(tokens1[1]);
		Integer year2 = Integer.valueOf(tokens2[1]);

		Integer month1 = Integer.valueOf(tokens1[2]);
		Integer month2 = Integer.valueOf(tokens2[2]);

		Integer naturalKeyPriority1 = (Integer)mSortingOrder.get(naturalKey1);
		Integer naturalKeyPriority2 = (Integer)mSortingOrder.get(naturalKey2);
		
		if (naturalKeyPriority1.compareTo(naturalKeyPriority2) == 0)
		{
			if (year1.compareTo(year2) == 0)
			{
				return month1.compareTo(month2) * -1;
			}
			return year1.compareTo(year1) * -1;
		}
		return naturalKeyPriority1.compareTo(naturalKeyPriority2);
	}
}