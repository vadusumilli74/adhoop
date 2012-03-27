import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ToNodeValues extends ArrayList<ToNodeValue> implements Writable
{
  public void readFields(DataInput in) throws IOException
  {
    int size = in.readInt();
    clear();
    ensureCapacity(size);
    for (int index = 0; index < size; index++)
    {
      ToNodeValue value = new ToNodeValue();
      value.readFields(in);
      add(index, value);
    }
  }

  public void write(DataOutput out) throws IOException
  {
    out.writeInt(size());
    for (int index = 0; index < size(); index++)
    {
      get(index).write(out);
    }
  }

  @Override
  public String toString()
  {
    String returnValue = "";
    for (int index = 0; index < size(); index++)
    {
      returnValue += " #" + index + "# " + get(index).toString();
    }
    
    return returnValue;
  }
}