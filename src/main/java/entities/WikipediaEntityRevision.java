package entities;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by besnik on 1/18/17.
 */
public class WikipediaEntityRevision implements Serializable, WritableComparable<WikipediaEntityRevision> {
    public long revision_id;
    public String user_id;
    public String user_name;
    public String revision_comment;
    public long timestamp;
    public WikipediaEntity entity;
    public long entity_id;

    public WikipediaEntityRevision() {
        entity = new WikipediaEntity();
    }

    @Override
    public int compareTo(WikipediaEntityRevision o) {
        return ((Long) (revision_id)).compareTo(((Long) o.revision_id));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.readFields(in);
    }
}
