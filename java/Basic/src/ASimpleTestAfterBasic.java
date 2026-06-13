import java.util.Scanner;


public class ASimpleTestAfterBasic {
    public static String[] toDo = new String[5];
    public static Scanner userInput = new Scanner(System.in);

    public static void addToDOList() {
        System.out.printf("Tambahkan Jadwal: ");
        String data = userInput.nextLine();
        var needMemory = true;
        for(var i = 0; i < toDo.length; i++) {
            if (toDo[i] == null) {
                needMemory = false;
                break;
            }
        }
        if (needMemory) {
            var temp = toDo;
            toDo = new String[5 * 2];
            for(var i = 0; i < temp.length; i++) {
                toDo[i] = temp[i];
            }
        }

        for (var i = 0; i < toDo.length; i++) {
            if (toDo[i] == null) {
                toDo[i] = data;
                break;
            }
        }
    }

    public static void deleteToDoList() {
        System.out.printf("Masukkan Nomor Urut Jadwal yang Dihapus: ");
        String _input = userInput.nextLine();
        int index = Integer.valueOf(_input);

        System.out.println(((index - 1) >= toDo.length));
        System.out.println((toDo[index - 1] == null));
        System.out.println(((index - 1) >= toDo.length) || (toDo[index - 1] == null));
        if (((index - 1) >= toDo.length) || (toDo[index - 1] == null)) {
            System.out.println("test 1");
            return;
        } else {
            System.out.println("test2");
            var temp = toDo;
            System.out.println(toDo.length);
            for(var i = index - 1; i < toDo.length; i++) {
                System.out.println("nilai i " + i);
                if (i == toDo.length - 1) {
                    toDo[i] = null;
                } else {
                    toDo[i] = temp[i + 1];
                }
            }
        }
    }

    public static void showToDOList() {
        var isHadSchedelu = false;
        for (var i = 0; i < toDo.length; i++ ) {
            var no = i + 1;
            if(toDo[i] != null) {
                if (!isHadSchedelu) {
                    isHadSchedelu = true;
                }
                System.out.println( no + ". " + toDo[i]);
            }
        }
        if(!isHadSchedelu) {
            System.out.println("Belum Ada Daftar Jadwal.");
        }
    }

    public static void main(String[] args) {


    }
}
