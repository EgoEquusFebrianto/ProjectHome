package tipeData;

public class DataType_Number {
    public static void main(String[] args) {
        // ini tipe data number
        byte thisByte = 100;
        short thisShort = 1000;
        int thisInt = 1000000;
        long thisLong = 1000000000;
        long thisLong2 = 1000000000L;

        // ini tipe data desimal
        float thisFloat = 12.24f;
        double thisDouble = 12.2424;

        // ini merupakan kode Literal
        int decimalInt = 25;
        int hexInt = 0xABC;
        int binInt = 0b010101;

        // underscore "_" dapat digunakan sebagai pemisah dalam angka
        short thisShort2 = 1_000;
        long thisLong3 = 1_000_000_000L;

        // Konversi Bilangan
        // urutan tipe data di java: byte - short - int - long - float - double

        // konversi dari kiri ke kanan dapat dilakukan secara otomatis, dengan kode sebagai berikut
        short konversiByte_Short = thisByte;
        int konversiShort_Int = konversiByte_Short;
        long konversiInt_Long = konversiShort_Int;

        // konversi dari kanan ke kiri harus dilakukan secara manual, dengan kode sebagai berikut
        int number = 50000;
        short konversiInt_Short = (short) number;
        byte konversiShort_Byte = (byte) konversiInt_Short;

        System.out.println(konversiInt_Short);
        System.out.println(konversiShort_Byte);
    }
}
