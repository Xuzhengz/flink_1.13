package com.xzz.chapter12;

/**
 * @author 徐正洲
 * @create 2022-10-10 10:36
 */
public class LoginEvent {
    private String userid;
    private String ip;
    private String event;
    private Long ts;

    public LoginEvent() {
    }

    public LoginEvent(String userid, String ip, String event, Long ts) {
        this.userid = userid;
        this.ip = ip;
        this.event = event;
        this.ts = ts;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userid='" + userid + '\'' +
                ", ip='" + ip + '\'' +
                ", event='" + event + '\'' +
                ", ts=" + ts +
                '}';
    }
}
