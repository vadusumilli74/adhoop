import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

//The Input Format is responsible for assigning the logical Input Splits to the individual Mapper.
//
//For the Mapper Algorithm used in this program the number of iteriation performed decreases exponentially from 2…N.
//
//For Example:
//For N = 100, Where
//Key = Number To Be Processed
//Iterations = Number of Iterations That Will Be Generated for the Key
//Processed = Number of Contiguous Keys To Be processed to reach N Iterations
//
//Key Iteriations Processed
//02   50            -
//03   33            2
//04   25            -
//05   20            -
//06   16            -
//07   14            -
//08   12            -
//09   11            6
//10   10            -
//…
//
//The above pattern continues as 2, 6, 16, 44, 120, 328, 896, 2448, 6688, 18272… ( (n-1) * (n-1) + (n-2) * (n-2) ).
//The Input Splits have to be arranged based on the above numbers which will give each Input Split approximately the same number of iterations. Based on the above pattern the input splits would be:
//Input Split 1 = 02 Numbers = 2, 3
//Input Split 2 = 06 Numbers = 4 … 9
//Input Split 3 = 16 Numbers = 10 … 25
//Input Split 4 = 44 Numbers = 26 … 69
//…
public class PrimeNumberInputFormat implements InputFormat<LongWritable, NullWritable>
{
  private long[] numbersAllowed = new long[22]; // 2, 6, 16, 44, 120, 328,
                                                // 896, 2448, 6688, 18272,
                                                // 49920, 136384, 372608,
                                                // 1017984, 2781184, 7598336,
                                                // 20759040, 56714752,
                                                // 154947584, 423324672,
                                                // 1156544512, 3159738368

  public PrimeNumberInputFormat()
  {
    numbersAllowed[0] = 2;
    numbersAllowed[1] = 6;
    for (int index = 2; index < numbersAllowed.length; index++)
    {
      numbersAllowed[index] = (numbersAllowed[index - 1] + numbersAllowed[index - 2]) * 2;
    }
  }

  public RecordReader<LongWritable, NullWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
  {
    return new PrimeNumberRecordReader((PrimeNumberInputSplit) split);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits)
  {
    long startingNumber = 2;
    long endingNumber = PrimeNumberDriver.MAXIMUM_INTEGER_VALUE;

    long runningTotal = 0;
    numSplits = 0;
    for (int index = 0; index < numbersAllowed.length && runningTotal < endingNumber; index++)
    {
      runningTotal = runningTotal + numbersAllowed[index];
      numSplits++;
    }

    long startingNumberInSplit = startingNumber;
    long endingNumberInSplit = 0;
    ArrayList<PrimeNumberInputSplit> splits = new ArrayList<PrimeNumberInputSplit>(numSplits);
    for (int index = 0; index < numSplits - 1; index++)
    {
      endingNumberInSplit = startingNumberInSplit + numbersAllowed[index];

      if (endingNumberInSplit > endingNumber)
      {
        break;
      }
      else
      {
        splits.add(new PrimeNumberInputSplit(startingNumberInSplit, endingNumberInSplit));

        startingNumberInSplit = endingNumberInSplit + 1;
      }
    }

    splits.add(new PrimeNumberInputSplit(startingNumberInSplit, endingNumber));

    return splits.toArray(new PrimeNumberInputSplit[splits.size()]);
  }

}