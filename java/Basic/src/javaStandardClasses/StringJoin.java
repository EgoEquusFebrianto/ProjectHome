package javaStandardClasses;

import java.util.StringJoiner;

public class StringJoin {
    public static void main(String[] args) {
        StringJoiner str = new StringJoiner(", ");

        str.add("mama");
        str.add("mia");
        str.add("lezatos");

        String value = str.toString();
        System.out.println(value);
    }
}
