import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

public class FromNodeKey implements WritableComparable<FromNodeKey>
{
  private int     node;
  private int     nodeWeight;
  private boolean discovered;
  private String  path;

  public FromNodeKey()
  {
    node = 0;
    nodeWeight = 0;
    discovered = false;
    path = "";
  }
  
  public void node(int value)
  {
    node = value;
  }
  
  public int node()
  {
    return node;
  }
  
  public void nodeWeight(int value)
  {
    nodeWeight = value;
  }
  
  public int nodeWeight()
  {
    return nodeWeight;
  }
  
  public void discovered(boolean value)
  {
    discovered = value;
  }
  
  public boolean discovered()
  {
    return discovered;
  }
  
  public void addPath(int value)
  {
    path += (path.trim().length() == 0 ? "" : "-") + value;
  }
  
  public boolean inPath(int value)
  {
    String [] nodes = path.split("-");
    for (int index = 0; index < nodes.length; index++)
    {
      if (nodes[index].equalsIgnoreCase(Integer.toString(value))) return true;
    }
    return false;
  }
  
  public void path(String value)
  {
    path = value;
  }
  
  public String path()
  {
    return path;
  }
  
  public void write(DataOutput out) throws IOException
  {
    out.writeInt(node);
    out.writeInt(nodeWeight);
    out.writeBoolean(discovered);
    out.writeUTF(path);
  }

  public void readFields(DataInput in) throws IOException
  {
    node = in.readInt();
    nodeWeight = in.readInt();
    discovered = in.readBoolean();
    path = in.readUTF();
  }

  public int compareTo(FromNodeKey w)
  {
    return (new Integer(node)).compareTo(new Integer(w.node));
  }
  
  @Override
  public String toString()
  {
    return node + "::" + nodeWeight + "::" + discovered + "::" + path;
  }
}