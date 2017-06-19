package mapred.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by besnik on 6/17/17.
 */
public class WikiText implements WritableComparable<WikiText>, Serializable {
    public long rev_id;
    public String text;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(rev_id);
        byte[] text_bytes = text.getBytes();
        out.writeInt(text_bytes.length);
        out.write(text_bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rev_id = in.readLong();
        byte[] text_bytes = new byte[in.readInt()];
        in.readFully(text_bytes);
        text = new String(text_bytes);
    }

    @Override
    public int compareTo(WikiText o) {
        return (rev_id < o.rev_id ? -1 : (rev_id == o.rev_id ? 0 : 1));
    }
}
