package lib.service;

public interface ToDoListService {
    void showToDoList();
    void addToDoList(String todo);
    void removeToDoList(Integer number);
    boolean getHaveList();
}
