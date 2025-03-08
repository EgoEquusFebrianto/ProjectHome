package lib.test;

import lib.repository.ToDoListRepository;
import lib.repository.ToDoListRepositoryImpl;
import lib.service.ToDoListService;
import lib.service.ToDoListServiceImpl;

public class serviceTest {
    public static void main(String[] args) {
        testRemoveToDoList();
    }

    public static void testShowToDoList() {
        ToDoListRepositoryImpl toDoListRepository = new ToDoListRepositoryImpl();
//        toDoListRepository.data[0] = new ToDoList("Belajar Java");
//        toDoListRepository.data[1] = new ToDoList("Belajar JavaStandard");
//        toDoListRepository.data[2] = new ToDoList("Belajar JavaOOP");

        ToDoListService toDoListService = new ToDoListServiceImpl(toDoListRepository);

        toDoListService.showToDoList();

    }

    public static void testAddToDoList() {
        ToDoListRepository toDoListRepository = new ToDoListRepositoryImpl();
        ToDoListService toDoListService = new ToDoListServiceImpl(toDoListRepository);

        toDoListService.addToDoList("Belajar Java");
        toDoListService.addToDoList("Belajar JavaStandard");
        toDoListService.addToDoList("Belajar JavaOOP");

        toDoListService.showToDoList();
    }

    public static void testRemoveToDoList() {
        ToDoListRepository toDoListRepository = new ToDoListRepositoryImpl();
        ToDoListService toDoListService = new ToDoListServiceImpl(toDoListRepository);

        toDoListService.addToDoList("Belajar Java");
        toDoListService.addToDoList("Belajar JavaStandard");
        toDoListService.addToDoList("Belajar JavaOOP");


        toDoListService.showToDoList();
        System.out.println();

        toDoListService.removeToDoList(3);
        toDoListService.showToDoList();
    }
}
