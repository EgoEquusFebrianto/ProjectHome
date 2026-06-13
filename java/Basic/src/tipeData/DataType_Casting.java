package tipeData;

public class DataType_Casting {
    public static void main(String[] args) {
        // ***** Konversi tipe data yang lengkap di Java

        // <> Konversi tipe data yang setara
        // 1. (auto-boxing), Primitif ke Non-Primitif
        int angkaInt0 = 10;
        Integer angkaObj = angkaInt0;

        // 2. (un-boxing) Non-Primitif ke Primitif
        Integer Integer1 = 10;
        int angkaPrimitif = Integer1;

        // <><> Konversi tipe data yang tidak setara
        // <> Primitif ke Primitif lainnya
        // Ingat urutan tipe data dari kecil ke besar: byte - short - int - long - float - double (Sama untuk yang Non-Primitif)

        // 1. Widening / (Implicit Cast), tipe data kecil ke besar
        int a = 100;
        double b = a;

        // 2. Narrowing / (Explicit Cast), tipe data besar ke kecil
        float x = 4.01F;
        int y = (int) x;

        // <> Non-Primitif ke Non-Primitif lainnya
        // baik dari kecil ke besar ataupun sebaliknya.
        Integer angka2 = 100;
        Long Long1 = angka2.longValue();

        // <> Primitif ke Non-Primitif
        int angka1 = 100;

        // 1. kecil ke besar
        Long angkaLong1 = Long.valueOf(angka1);
        Long angkaLong3 = (long) angka1;

        // 2. besar ke kecil
        Short angkaShort1 = Short.valueOf((short) angka1);
        Short angkaShort2 = (short) angka1;


        // <> Non-primitif ke Primitif
        Integer angka3 = 1000;

        // 1. kecil ke besar
        long angkaLong = angka3.longValue();


        // 2. besar ke kecil
        short angkaShort = angka3.shortValue();

        System.out.println(angkaLong);
        System.out.println(angkaShort);
    }
}