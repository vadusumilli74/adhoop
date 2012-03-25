import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapred.RecordReader;

public class PrimeNumberRecordReader implements RecordReader<LongWritable, NullWritable>
{
  private long currentNumber;
  private long endingNumber;
  private long startingNumber;

  public PrimeNumberRecordReader(PrimeNumberInputSplit split)
  {
    this.currentNumber = split.getStartingNumber();
    this.endingNumber = split.getEndingNumber();
    this.startingNumber = split.getStartingNumber();
  }

  public void close()
  {
  }

  public LongWritable createKey()
  {
    return new LongWritable();
  }

  public NullWritable createValue()
  {
    return NullWritable.get();
  }

  public long getPos()
  {
    return this.endingNumber - this.currentNumber;
  }

  public float getProgress()
  {
    return Math.min(1.0f, (this.currentNumber - this.startingNumber) / (float) (this.endingNumber - this.startingNumber));
  }

  public boolean next(LongWritable key, NullWritable value)
  {
    if (this.currentNumber <= this.endingNumber)
    {
      key.set(this.currentNumber);
      this.currentNumber++;

      return true;
    }
    else
    {
      return false;
    }
  }
}
