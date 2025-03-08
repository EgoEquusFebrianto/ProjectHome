package lib.entity;

public class ToDoList {
    private String todo;

    public ToDoList(String _todo) {
        todo = _todo;
    }

    public ToDoList() {}

    public String getTodo() {
        return todo;
    }

    public void setTodo(String todo) {
        this.todo = todo;
    }
}
