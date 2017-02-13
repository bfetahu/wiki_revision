package entities;

/**
 * Created by besnik on 1/18/17.
 */
public class WikipediaEntityRevision {
    public long revision_id;
    public long user_id;
    public String revision_comment;
    public long timestamp;
    public WikipediaEntity entity;
    public long entity_id;

    public WikipediaEntityRevision() {
        entity = new WikipediaEntity();
    }
}
