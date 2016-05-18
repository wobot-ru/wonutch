package ru.wobot.index.flink;

import java.io.Serializable;

/**
 * Created by kviz on 4/30/2016.
 */
public class PostDetails implements Serializable {
    private static final long serialVersionUID = -8775358157899L;

    public PostDetails() {
        id="";
        crawlDate="";
        digest="";
        score="";
        segment="";
        source="";
        isComment=false;
        engagement=0;
        parentPostId="";
        body="";
        date="";
        href="";
        smPostId="";
        city="";
        gender="";
        profileHref="";
        profileId="";
        profileName="";
        reach=0;
        smProfileId="";
    }

    public String id;
    public String crawlDate;
    public String digest;
    public String score;
    public String segment;
    public String source;
    public Boolean isComment;
    public long engagement;
    public String parentPostId;
    public String body;
    public String date;
    public String href;
    public String smPostId;
    public String city;
    public String gender;
    public String profileHref;
    public String profileId;
    public String profileName;
    public long reach;
    public String smProfileId;
}
