package entities;

import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by besnik on 4/5/17.
 */
public class WikiEntity implements Serializable, Writable {
    public long revision_id;
    public String user_id;
    public long entity_id;
    public String timestamp;
    public String title;

    public Map<String, WikiSectionSimple> sections;

    public WikiEntity() {
        sections = new HashMap<>();
    }

    public void generateSectionBoW() {
        if (sections != null && !sections.isEmpty()) {
            sections.keySet().forEach(s -> sections.get(s).generateBoW());
        }
    }

    public byte[] getBytes() throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(this);

        byte[] bytes = b.toByteArray();
        System.out.printf("The object for entity %s is of length %d\n", title, bytes.length);
        return bytes;
    }

    public void readBytes(byte[] bytes) throws IOException {
        try {
            ObjectInputStream obj = new ObjectInputStream(new ByteArrayInputStream(bytes));
            WikiEntity rev = (WikiEntity) obj.readObject();

            System.out.printf("The object for entity %s is of length %d\n", rev.title, bytes.length);
            this.revision_id = rev.revision_id;
            this.user_id = rev.user_id;
            this.timestamp = rev.timestamp;
            this.entity_id = rev.entity_id;
            this.sections = rev.sections;
            this.title = rev.title;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = getBytes();
        out.writeInt(bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        readBytes(bytes);
    }
}
