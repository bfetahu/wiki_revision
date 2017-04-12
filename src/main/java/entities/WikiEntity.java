package entities;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by besnik on 4/5/17.
 */
public class WikiEntity implements Serializable {
    public long revision_id;
    public String user_id;
    public long entity_id;
    public String timestamp;
    public String title;

    public Map<String, WikiSectionSimple> sections;

    public WikiEntity() {
        sections = new HashMap<>();
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
}
