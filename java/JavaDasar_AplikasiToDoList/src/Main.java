import java.util.Scanner;

public class Main {
    public static String[] toDo = new String[10];
    public static Scanner _input = new Scanner(System.in);

    public static void showToDoList() {
        boolean counter = false;
        for (var i = 0; i < toDo.length ; i++) {
            var todo = toDo[i];
            var no = i + 1;

            if (todo != null) {
                if(!counter) {
                    counter = true;
                }
                System.out.println(no + ". " + todo);
            }
        }

        if (!counter) {
            System.out.println("List Jadwal Tidak Ada." + "\n");
        }
    }

    public static void addToDoList() {
        System.out.printf("Masukkan Jadwal Anda: ");
        String todo = _input.nextLine();
        var onMemory = true;

        System.out.println("test1");
        for (var i = 0; i < toDo.length; i++) {
            if(toDo[i] == null) {
                onMemory = false;
                break;
            }

        }

        System.out.println("test2");
        if (onMemory) {
            var temp = toDo;
            toDo = new String[toDo.length * 2];

            for (var i = 0; i < temp.length; i++) {
                toDo[i] = temp[i];
            }
        }

        System.out.println("test3");
        for (var i = 0; i < toDo.length; i++) {
            if (toDo[i] == null) {
                toDo[i] = todo;
                break;
            }
        }

    }

    public static void removeToDoList() {
        System.out.printf("Masukkan Urutan Jadwal yang akan Dihapus: ");
        int num = Integer.parseInt(_input.nextLine());

        if (((num - 1) >= toDo.length) || (toDo[num - 1] == null)) {
            return;
        } else {
            for (var i = (num - 1); i < toDo.length; i++) {
                if (i == (toDo.length - 1)) {
                    toDo[i] = null;
                } else {
                    toDo[i] = toDo[i + 1];
                }
            }
        }
    }

    public static String appViews() {
        System.out.println("Menu");
        System.out.println("1. Tampilkan Daftar Kegiatan.");
        System.out.println("2. Jadwalkan Kegiatan.");
        System.out.println("3. Hapus Jadwal Kegiatan.");
        System.out.println("4. Keluar." + "\n");

        System.out.printf("Masukkan Pilihan Anda: ");
        return _input.nextLine();
    }

    public static void run() {
        while (true) {
            String inputMenu = appViews();
            if (inputMenu.equals("1")) {
                showToDoList();
            } else if (inputMenu.equals("2")) {
                addToDoList();
            } else if (inputMenu.equals("3")) {
                removeToDoList();
            } else if (inputMenu.equals("4")) {
                break;
            } else {
                System.out.println("Input Tidak Dikenal.");
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("Selamat Datang Di Aplikasi ToDO List");
        run();
    }
}