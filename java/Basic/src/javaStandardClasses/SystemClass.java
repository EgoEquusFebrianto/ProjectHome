package javaStandardClasses;

public class SystemClass {
    public static void main(String[] args) {
        // System Class banyak memiliki utility static method yang penting, bisa lihat docs
        // beberapa contoh utility

        System.out.println(System.currentTimeMillis());
        System.out.println(System.nanoTime());
        System.out.println(System.getenv("Java_Home"));

        System.gc();
        System.exit(0);

        System.out.println("Hi, Good Moring");
    }
}
