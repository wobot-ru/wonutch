package ru.wobot.flink;

public class Post extends Document {
    public String profileId;
    public long smPostId;
    public String parentPostId;
    public String body;
    public String date;
    public int engagement;
    public Boolean isComment;
    public String authorName;
}
