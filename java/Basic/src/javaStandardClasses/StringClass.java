package javaStandardClasses;

public class StringClass {
    public static void main(String[] args) {
        // String termasuk tipe immutable
        String name = "Febrianto Kudadiri";
        String nameLowerCase = name.toLowerCase();
        String nameUpperCase = name.toUpperCase();
        String[] nameArray = name.split(" ");

        System.out.println(name);
        System.out.println(nameLowerCase);
        System.out.println(nameUpperCase);
        System.out.println(name.length());
        System.out.println(name.startsWith("Feb"));
        System.out.println(name.endsWith("iri"));
        for (String space : nameArray) {
            System.out.println(space);
        }
    }
}
