package ru.wobot.flink;

public interface ProfileProperties {
    String SOURCE = "source";
    String HREF = "href";
    String SM_PROFILE_ID = "sm_profile_id";
    String NAME = "name";
    String CITY = "city";
    /**
     * Reach is the potential audience for a message based on total follower count (Twitter, Pinterest and LinkedIn followers, total Likes on your Facebook page, etc).
     *
     * @see <a href="https://wobot-ru.atlassian.net/wiki/x/FAAq">Охват</a>
     */
    String REACH = "reach";
    String FRIEND_COUNT = "friend_count";
    String FOLLOWER_COUNT = "follower_count";
    String GENDER = "gender";
    String SEGMENT = "segment";
    String DIGEST = "digest";
}