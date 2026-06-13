package tipeData;

public class DataType_NotPrimitive {
    public static void main(String[] args) {
        // di Java, disebut tipe data primitif karena bukan dalam bentuk Objek
        // perlu diingat lagi bahwa Java merupakan Object Oriented Programming

        // Representasinya

        // Number
        // tipe primitif: byte - short - int - long - float - double
        // tipe bukan primitif: Byte - Short - Integer - Long - Float - Double

        // lain-lainnya
        // tipe primitif: char dan boolean
        // tipe bukan primitif: Character dan Boolean

        // bagaimana dengan "String"?
        // String bukan tipe data primitif
        // setiap tipe data primitif, tidak memiliki nilai default atau akan memberi error bila tidak disetting
        // tipe data bukan primitif, bisa memiliki method/function

        Short iniShort = 1000;
        Integer iniInteger = 100000;

        // Konversi data Primitif -> bukan Primitif
        int primitif = 100000;
        Integer nonPrimitif = primitif;

        // Konversi untuk sebaliknya
        Integer nonPrimitif2 = 100000;
        int PrimitifAgain = nonPrimitif2;

        System.out.println(PrimitifAgain);

        // bagaimana untuk tipe yang berbeda?
        byte PrimitifByte = nonPrimitif2.byteValue();
        short PrimitifShort = nonPrimitif2.shortValue();

        System.out.println(PrimitifByte);
        System.out.println(PrimitifShort);
    }
}
