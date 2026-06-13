package tipeData;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class DataType_Map {
    public static void main(String[] args) {
        // Jenis-Jenis Map yang dapat di-Update <Mutable>

        // 1. HashMap ==> Tidak memiliki Urutan, Kecepatan pencarian O(1), Performa lebih baik
//        Map<String, String> map = new HashMap<>();

//        map.put("1", "Mennie");
//        map.put("2", "Minie");
//        map.put("3", "Moo");
//
//        System.out.println(map);
//
//        map.remove("2");
//        map.put("4", "Sten lee");
//
//        System.out.println(map);

        // 2. LinkedHashMap ==> Urutan sesuai Input, Lebih lambat, Tujuan menjaga urutan input
//        Map<String, String> map1 = new LinkedHashMap<>();
//
//        map1.put("1", "Mennie");
//        map1.put("2", "Minie");
//        map1.put("3", "Moo");
//
//        System.out.println(map1);
//
//        map1.remove("2");
//        map1.put("4", "Stan lee");
//        System.out.println(map1);

        // 3. TreeMap ==> terurut berdasarkan key, Jauh lebih lambat dengan komputasi O(log n), Urutan Key selalu terurut
//        Map<String, String> map2 = new TreeMap<>();

//        map2.put("1", "Mennie");
//        map2.put("2", "Minie");
//        map2.put("3", "Moo");
//
//        System.out.println(map2);
//
//        map2.remove("2");
//        map2.put("4", "Stan lee");
//        System.out.println(map2);


        // Jenis-Jenis Map yang tidak dapat diubah

        // 1. Map.of() => Singkat Dan cepat, Namun maksimal element data 10
//        Map<String, String> map3 = Map.of(
//                "1", "Apple",
//                "2", "Banana",
//                "3", "Watermelon",
//                "4", "Watermelon",
//                "5", "Watermelon",
//                "6", "Watermelon",
//                "7", "Watermelon",
//                "8", "Watermelon",
//                "9", "Watermelon",
//                "10", "Watermelon"
//        );
//
//        System.out.println(map3);

        // 2. Map.ofEntries() mirip seperti Map.of(), tapi sedikit lebih lambat dan Element bisa lebih dari 10
//        Map<String, String> map4 = Map.ofEntries(
//                Map.entry("1", "Apple"),
//                Map.entry("2", "Banana"),
//                Map.entry("3", "Watermelon")
//        );
//
//        System.out.println(map4);

        // Kesimpulan,
        // 1. hanya 2 metode yang menghasilkan Map dengan elemen yang terutut
        // 2. terdapat 2 metode yang elemen datanya tidak dapat di manipulasi
    }
}
