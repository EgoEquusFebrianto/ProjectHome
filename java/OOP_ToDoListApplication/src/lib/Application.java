package lib;

import lib.view.ToDoListView;
import java.util.Scanner;

public class Application {
    Scanner _input =  new Scanner(System.in);
    ToDoListView toDoListView = new ToDoListView();

    public void run() {
        while (true) {
            System.out.println("\n=== MENU TO-DO LIST ===");
            System.out.println("1. Tampilkan Jadwal Kegiatan.");
            System.out.println("2. Tambahkan Jadwal Kegiatan.");
            System.out.println("3. Hapus Jadwal Kegiatan.");
            System.out.println("4. Keluar.\n");
            System.out.print("Pilih menu: ");
            String choiceServices = _input.nextLine().trim();

            switch (choiceServices) {
                case "1":
                    toDoListView.showToDoList();
                    break;
                case "2":
                    toDoListView.addToDoList();
                    break;
                case "3":
                    toDoListView.removeToDoList();
                    break;
                case "4":
                    System.out.println("Keluar dari aplikasi...");
                    return;
                default:
                    toDoListView.wrongSelectServices();
            }
        }
    }
}