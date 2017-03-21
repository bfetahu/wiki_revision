package entities;

import java.io.Serializable;

/**
 * Created by besnik on 1/18/17.
 */
public class WikipediaEntityRevision implements Serializable {
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
}
