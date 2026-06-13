package lib.view;

import lib.repository.ToDoListRepository;
import lib.repository.ToDoListRepositoryImpl;
import lib.service.ToDoListService;
import lib.service.ToDoListServiceImpl;
import java.util.Scanner;

public class ToDoListView {
    ToDoListRepository toDoListRepository = new ToDoListRepositoryImpl();
    ToDoListService toDoListService = new ToDoListServiceImpl(toDoListRepository);
    Scanner _input = new Scanner(System.in);

    public void showToDoList() {
        System.out.println("Daftar Kegiatan Untuk Hari ini: \n");
        toDoListService.showToDoList();

        System.out.print("\nTekan Tombol Enter untuk Melanjutkan ");
        _input.nextLine();
        System.out.println();
    }

    public void addToDoList() {
        System.out.print("Masukkan Jadwal Kegiatan: ");
        String todo = _input.nextLine();

        toDoListService.addToDoList(todo);
        System.out.println("Jadwal Behasil Ditambahkan ke Daftar Kegiatan.\n");
    }

    public void removeToDoList() {
        toDoListService.showToDoList();
        if (toDoListService.getHaveList()) {
            System.out.print("\nMasukkan Jadwal Kegiatan yang akan Dihapus: ");
            Integer todo = _input.nextInt();

            toDoListService.removeToDoList(todo);
            System.out.println("Jadwal Behasil Dihapus dari Daftar Kegiatan.\n");
        }
    }

    public void wrongSelectServices() {
        System.out.println("Pilihan Tidak Dikenali.");
        System.out.print("\nTekan Tombol Enter untuk Melanjutkan ");
        _input.nextLine();
        System.out.println();
    }
}
