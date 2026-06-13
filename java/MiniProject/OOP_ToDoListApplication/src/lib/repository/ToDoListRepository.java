package lib.repository;

import lib.entity.ToDoList;

public interface ToDoListRepository {
    ToDoList[] getAll();

    void add(ToDoList todolist);
    void remove(Integer number);
}
