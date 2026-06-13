package lib.service;

import lib.entity.ToDoList;
import lib.repository.ToDoListRepository;

public class ToDoListServiceImpl implements ToDoListService{
    private ToDoListRepository todolistrepository;
    private Boolean isHavingList;

    public ToDoListServiceImpl(ToDoListRepository _todolistrepository) {
        todolistrepository = _todolistrepository;
    }

    @Override
    public void showToDoList() {
        ToDoList[] model = todolistrepository.getAll();

        boolean counter = false;
        isHavingList = true;
        for (var i = 0; i < model.length ; i++) {
            var todo = model[i];
            var no = i + 1;

            if (i == 0 && todo == null) {
                break;
            } else if (todo != null) {
                if(!counter) {
                    counter = true;
                }
                System.out.println(no + ". " + todo.getTodo());
            }
        }

        if (!counter) {
            System.out.println("List Jadwal Tidak Ada.");
            isHavingList = false;
        }
    }

    @Override
    public void addToDoList(String todo) {
        ToDoList toDoList = new ToDoList(todo);
        todolistrepository.add(toDoList);

    }

    @Override
    public void removeToDoList(Integer number) {
        todolistrepository.remove(number);
    }

    @Override
    public boolean getHaveList() {
        return isHavingList;
    }
}
