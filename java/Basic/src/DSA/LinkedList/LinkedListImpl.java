package DSA.LinkedList;

public class LinkedListImpl {
    public static void main(String[] args) {
        LinkedList linkedList = new LinkedList();

        linkedList.append(1);
        linkedList.append(2);
        linkedList.append(3);
        linkedList.append(4);
        linkedList.insert(0);
        linkedList.print();

//        linkedList.removeHead();
//        linkedList.print();
//
//        linkedList.removeTail();
//        linkedList.print();

        linkedList.remove(0);
        linkedList.print();
        linkedList.length();
    }
}
