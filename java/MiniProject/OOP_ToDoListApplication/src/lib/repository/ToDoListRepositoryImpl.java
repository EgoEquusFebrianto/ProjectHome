package lib.repository;
import lib.entity.ToDoList;

public class ToDoListRepositoryImpl implements ToDoListRepository{

    private ToDoList[] data = new ToDoList[10];

    @Override
    public ToDoList[] getAll() {return data;}

    boolean onMemory() {
        var  onMemory = true;

        for (ToDoList datum : data) {
            if (datum == null) {
                onMemory = false;
                break;
            }

        }
        return onMemory;
    }

    void resizeMemory() {
        if (onMemory()) {
            var temp = data;
            data = new ToDoList[data.length * 2];

//            for (var i = 0; i < temp.length; i++) {
//                data[i] = temp[i];
//            }

            System.arraycopy(temp, 0, data, 0, temp.length);
        }
    }

    @Override
    public void add(ToDoList todolist) {
        resizeMemory();

        for (var i = 0; i < data.length; i++) {
            if (data[i] == null) {
                data[i] = todolist;
                break;
            }
        }
    }

    @Override
    public void remove(Integer num) {
        if (((num - 1) >= data.length) || (data[num - 1] == null)) {
            System.out.println("Data Tidak Ditemukan.\n");
            return;
        } else {
            for (var i = (num - 1); i < data.length; i++) {
                if (i == (data.length - 1)) {
                    data[i] = null;
                } else {
                    data[i] = data[i + 1];
                }
            }
        }
    }
}
