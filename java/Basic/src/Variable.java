public class Variable {
    public static void main(String[] args) {
        String Name;
        Name = "Budi Otonom";
        System.out.println(Name);

        // penamaan tipe variabel dapat dilakukan secara otomatis dengan menggunakan var
        // namun, perlu diingat penggunaan var harus memberi value/nilai langsung ke variabel

        var Names = "Febrianto Kudadiri";
        var umur = 20;
        var status = false;

        System.out.println(Names);
        System.out.println(umur);
        System.out.println(status);

        // untuk mencegah nilai variabel berubah, dapat menggunakan final
        // jika variabel dengan tipe data ini diubah, maka akan mengembalikan hasil error

        final var bahasa = "Java";
        System.out.println(bahasa);
        // bahasa = "Python"; // error
    }
}
