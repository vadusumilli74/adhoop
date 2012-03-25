import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.mapred.InputSplit;

public class PrimeNumberInputSplit implements InputSplit
{
  private long startingNumber;
  private long endingNumber;

  public PrimeNumberInputSplit()
  {
  }

  public PrimeNumberInputSplit(long start, long end)
  {
    this.startingNumber = start;
    this.endingNumber = end;
  }

  public long getLength()
  {
    return (this.endingNumber - this.startingNumber) * 8;
  }

  public String[] getLocations() throws IOException
  {
    return new String[] {};
  }

  public void readFields(DataInput in) throws IOException
  {
    this.startingNumber = in.readLong();
    this.endingNumber = in.readLong();
  }

  public void write(DataOutput out) throws IOException
  {
    out.writeLong(this.startingNumber);
    out.writeLong(this.endingNumber);
  }

  public long getStartingNumber()
  {
    return this.startingNumber;
  }

  public long getEndingNumber()
  {
    return this.endingNumber;
  }

}
