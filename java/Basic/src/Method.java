public class Method {
    // kembali ingat, Java merupakan Object Oriented Programming
    // jadi nama fungsionalitasnya disebut method untuk bentuk class

    // tanpa parameter
    static void sayHello() {
        System.out.println("Hello Boys");
    }

    // dengan parameter
    static void name(String firstName, String lastName) {
        System.out.println("Hello my name " + firstName + " " + lastName);
    }

    // method return value
    static String checkNumber(int num) {
        var hasil = num % 2 == 1 ? "this number is odd":"this number is even";
        return hasil;
    }

    static String CheckNilaiAngka(int num) {
        String hasil = num >= 90 ? "A" : num >= 80 ? "B" : num >= 70 ? "C" : num >= 60 ? "D" : "E";
        return hasil;
    }

    // method variable argument, ini mirip dengan arbitrary(args) argumen di Python
    static void checkNilai(String agregasi, int... values) {
        var nilai = 0;
        for(var value : values) {
            nilai += value;
        }

        var hasil = nilai / values.length;
        if (hasil >= 70) {
            System.out.println("agregasi " + agregasi + ", Sesuai Harapan");
        } else {
            System.out.println("agregasi " + agregasi + ", Tidak Sesuai Harapan");
        }
    }

    public static void main(String[] args) {
        sayHello();
        name("Febrianto", "Kudadiri");
        System.out.println(checkNumber(3));
        System.out.println(checkNumber(4));
        checkNilai("Average", 80, 80, 80, 80, 80);
        CheckNilaiAngka(80);
    }

}
