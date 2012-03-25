import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

//Mapper:
//
//  Known Primes (Kp): 2, 3, 5, 7
//
//  STEP A: For every Key n = {2… N}
//  ==> Emit Key-Value Pair {K, V} = {n, 1}
//
//  AND
//
//  STEP B: For every Key n = a Kp OR n % every Kp != 0
//  ==> Emit Key-Value Pair {K, V} = {n x f, 1} For Every f = {2…N / n} & n x f <= N
//
//  The algorithm will work with out the concept of Known Primes but will emit more unnecessary Key-Value Pairs than needed. The use of Known primes hace decreased the amount of File Bytes Written by the Mapper by 80%.
//
//  Mapper Output:
//
//  n = 02: Step A & B: Key-Value Pairs Emmited: (2,1) (4,1) (6,1) (8,1) (10,1)
//  n = 03: Step A & B: Key-Value Pairs Emmited: (3,1) (6,1) (9,1)
//  n = 04: Step A: Key-Value Pairs Emmited: (4,1)
//  n = 05: Step A & B: Key-Value Pairs Emmited: (5,1) (10,1)
//  n = 06: Step A: Key-Value Pairs Emmited: (6,1)
//  n = 07: Step A & B: Key-Value Pairs Emmited: (7,1)
//  n = 08: Step A: Key-Value Pairs Emmited: (8,1)
//  n = 09: Step A: Key-Value Pairs Emmited: (9,1)
//  n = 10: Step A: Key-Value Pairs Emmited: (10,1)
// 
public class PrimeNumberMapper extends MapReduceBase implements
    Mapper<LongWritable, NullWritable, LongWritable, ByteWritable>
{
  private static final long[] knownPrimes = { 2, 3, 5, 7 };
  
  private ByteWritable one = new ByteWritable((byte)1);

  private boolean isKnownPrime(long number)
  {
    boolean knownPrime = false;
    boolean notKnownPrimeProduct = true;

    for (int index = 0; index < knownPrimes.length; index++)
    {
      knownPrime = !knownPrime ? (number == knownPrimes[index]) : knownPrime;
      notKnownPrimeProduct = notKnownPrimeProduct ? ((number % knownPrimes[index]) != 0) : notKnownPrimeProduct;
    }

    return knownPrime || notKnownPrimeProduct;
  }

  @Override
  public void map(LongWritable key, NullWritable value, OutputCollector<LongWritable, ByteWritable> output,
      Reporter reporter) throws IOException
  {
    output.collect(key, one);

    long fromNumber = key.get();
    if (isKnownPrime(fromNumber))
    {
      long maximumFactor = PrimeNumberDriver.MAXIMUM_INTEGER_VALUE / fromNumber;
      for (long factor = 2; factor <= maximumFactor; factor++)
      {
        output.collect(new LongWritable(fromNumber * factor), one);
      }
    }
  }
}
