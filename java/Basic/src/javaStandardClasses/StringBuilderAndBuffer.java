package javaStandardClasses;

public class StringBuilderAndBuffer {
    public static void main(String[] args) throws InterruptedException {
        // Karena String adalah tipe data immutable, untuk memanipulasi string yang banyak,
        // Tidak disarankan menggunakan String, karena akan mengkonsumsi banyak memori
        // Solusi: Gunakan StringBuffer atau StringBuilder
        // Perbedaannya:
        // 1. StringBuffer -> Thread Safe, StringBuilder -> Tidak Thread Safe
        // 2. StringBuffer dapat memanipulasi string secara pararel
        // 3. Karena Thread safe, StringBuffer lebih lambat dengan StringBuilder
        // Catatan, Thread Safe -> proses aman untuk menjalankan banyak thread secara bersamaan (pararel)
        // tanpa menyebabkan race condition

        StringBuilder builder = new StringBuilder(); // Implementasi sama untuk StringBuffer
        builder.append("Desmond");
        builder.append("Meridian");
        builder.append("Valirian");

        String str = builder.toString();
        System.out.println(str);

        // Contoh demonstrasi pararelisme di StringBuffer
        StringBuffer stringBuffer = new StringBuffer("Hello");

        Runnable task = () -> {
            for (int i = 0; i < 5 ; i++) {
                stringBuffer.append(" World");
            }
        };

        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        String res = stringBuffer.toString();
        System.out.println(res);

    }
}
