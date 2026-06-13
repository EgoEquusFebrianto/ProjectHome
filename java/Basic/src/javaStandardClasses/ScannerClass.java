package javaStandardClasses;

import java.util.Scanner;

public class ScannerClass {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);

        System.out.print("Name: ");
        String name = scan.nextLine();

        System.out.print("Age: ");
        Integer age = scan.nextInt();

        System.out.printf("Hello %s, Umur anda %d", name, age);
    }
}
