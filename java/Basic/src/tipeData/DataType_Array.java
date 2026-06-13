package tipeData;

import java.util.List;
import java.util.ArrayList;

public class DataType_Array {
    public static void main(String[] args) {
        // Contoh 1
        String[] ArrayString = new String[3];
        ArrayString[0] = "Febrianto";
        ArrayString[1] = "Kudadiri";

        // contoh 2
        short[] ArrayShort = new short[] {
                10, 20, 30, 40, 50
        };

        // Contoh 3
        int[] ArrayInt = {
                10, 20, 30, 40, 50, 60
        };

        // Contoh 4
        List<Integer> ArrayInteger = new ArrayList<>();
        ArrayInteger.add(10);
        ArrayInteger.add(20);
        ArrayInteger.add(30);
        ArrayInteger.add(40);
        ArrayInteger.add(50);

        // Contoh 5
        Object[] example = {10, 20, null, false, "String"};

        // Operasi Array
        System.out.println(ArrayInt);
        System.out.println(ArrayInt[1]); // mengambil nilai dalam array
        ArrayInt[0] = 100; // mengubah nilai dalam array
        System.out.println(ArrayInt.length); // mengembalikan panjang array

        // array 2 Dimensi

        String[][] members = {
                {"Dugong", "Monate"},
                {"Dilukh", "Samba"},
                {"Rion", "Berrandikh"}
        };

        System.out.println(members[0][1]);
        System.out.println(members[1][1]);
        System.out.println(example[2]);
        System.out.println(ArrayInteger.get(0));
    }
}
