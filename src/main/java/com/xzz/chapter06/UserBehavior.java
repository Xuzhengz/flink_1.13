package com.xzz.chapter06;

/**
 * @author 徐正洲
 * @create 2022-09-07 17:21
 */
public class UserBehavior {
    private Long userid;
    private Long itemid;
    private Long categoryid;
    private String behavior;
    private Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(Long userid, Long itemid, Long categoryid, String behavior, Long timestamp) {
        this.userid = userid;
        this.itemid = itemid;
        this.categoryid = categoryid;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserid() {
        return userid;
    }

    public void setUserid(Long userid) {
        this.userid = userid;
    }

    public Long getItemid() {
        return itemid;
    }

    public void setItemid(Long itemid) {
        this.itemid = itemid;
    }

    public Long getCategoryid() {
        return categoryid;
    }

    public void setCategoryid(Long categoryid) {
        this.categoryid = categoryid;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userid=" + userid +
                ", itemid=" + itemid +
                ", categoryid=" + categoryid +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
