package lib.implement;

import lib.algorithm.EnumClass;

public class EnumApp {
    public static void main(String[] args) {
        Customers customer = new Customers();

        customer.setName("Kudadiri");
        customer.setLevel(EnumClass.Standard);

        System.out.println(customer.getName());
        System.out.println(customer.getLevel());
        System.out.println(customer.getLevel().getDesc());

        String level = customer.getLevel().name();

        EnumClass takeLevel = EnumClass.valueOf("VIP");
        EnumClass[] arrayEnums = EnumClass.values();

        System.out.println(level);
        System.out.println(takeLevel);
        System.out.println(arrayEnums);
    }
}
