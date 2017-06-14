package mapred.io;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by besnik on 14.06.17.
 */
public class TextList implements Writable, Serializable {
    private List<String> values;
    public TextList(){
        values = new ArrayList<>();
    }

    public List<String> getValues(){
        return values;
    }

    public int getSize(){
        return values.size();
    }

    public String getItem(int index){
        if(index >= values.size()){
            return null;
        }
        return values.get(index);
    }

    public void add(String value){
        values.add(value);
    }
    @Override
    public void write(DataOutput out) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(values);

        byte[] bytes= b.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        byte[] bytes = new byte[size];
        in.readFully(bytes);

        ObjectInputStream bi = new ObjectInputStream(new ByteArrayInputStream(bytes));
        try{
            values = (List)bi.readObject();
        }catch (Exception e){
            System.out.println("Error casting/reading object " + e.getMessage());
        }

    }
}
