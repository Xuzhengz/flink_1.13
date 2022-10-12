package com.xzz.chapter12;

/**
 * @author 徐正洲
 * @create 2022-10-12 10:19
 */
public class Order {
    private String userId;
    private String orderId;
    private String eventType;
    private Long ts;

    public Order() {
    }

    public Order(String userId, String orderId, String eventType, Long ts) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.ts = ts;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Order{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ts=" + ts +
                '}';
    }
}
