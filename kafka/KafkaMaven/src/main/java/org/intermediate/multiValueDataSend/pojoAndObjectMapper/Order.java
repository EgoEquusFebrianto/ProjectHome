package org.intermediate.multiValueDataSend.pojoAndObjectMapper;

import java.io.Serializable;
import java.util.List;

public class Order implements Serializable {
    private String orderID;
    private String customerName;
    private List<String> items;
    private int totalPrice;

    public Order() {}

    public Order(String orderID, String customerName, List<String> items, int totalPrice) {
        this.orderID = orderID;
        this.customerName = customerName;
        this.items = items;
        this.totalPrice = totalPrice;
    }

    public String getOrderID() {
        return orderID;
    }

    public String getCustomerName() { return customerName; }

    public List<String> getItems() {
        return items;
    }

    public int getTotalPrice() {
        return totalPrice;
    }
}
