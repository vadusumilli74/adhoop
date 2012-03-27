import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ToNodeValue implements Writable
{
  private int     node;
  private int     edgeWeight;

  public ToNodeValue()
  {
    node = 0;
    edgeWeight = 0;
  }
  
  public void node(int value)
  {
    node = value;
  }
  
  public int node()
  {
    return node;
  }
  
  public void edgeWeight(int value)
  {
    edgeWeight = value;
  }
  
  public int edgeWeight()
  {
    return edgeWeight;
  }
  
  public void write(DataOutput out) throws IOException
  {
    out.writeInt(node);
    out.writeInt(edgeWeight);
  }

  public void readFields(DataInput in) throws IOException
  {
    node = in.readInt();
    edgeWeight = in.readInt();
  }
  
  @Override
  public String toString()
  {
    return node + "::" + edgeWeight;
  }
}