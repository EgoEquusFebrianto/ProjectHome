package DSA.LinkedList;

public class LinkedList {
    private Node head;
    private int size;

    public void append(Object data) {
        Node node = new Node(data);
        if (head == null) {
            head = node;
        } else {
            Node current = head;
            while (current.getNext() != null) {
                current = current.getNext();
            }
            current.setNext(node);
        }
        size += 1;
    }

    public void insert(Object data) {
        Node node = new Node(data);
        if (head == null) {
            head = node;
        } else {
            Node temp = head;
            head = node;
            head.setNext(temp);
        }
        size += 1;
    }

    public void removeHead() {
        head = head.getNext();
        size -= 1;
    }

    public void removeTail() {
        Node temp = head.getNext();

        while(temp.getNext() != null) {
            if (temp.getNext().getNext() == null) {
                temp.setNext(null);
                break;
            }
            temp = temp.getNext();

        }
        size -= 1;
    }

    public void remove(Object data) {
        if (head == null) {
            System.out.println("Linked List doesn't have data.");
        } else if (head.getNext() == null) {
            if (head.getData() == data) {
                head = null;
            } else {
                System.out.println("Data not founded.");
            }
        } else {
            Node prev = head;
            Node current = head;
            Node temp = null;
            boolean loop = true;

            while (loop) {
                if (current.getData() == data && current.getNext() != null) {
                    temp = current.getNext();
                    if (current == prev) {
                        head = temp;
                    } else {
                        prev.setNext(temp);
                    }
                    loop = false;
                } else if (current.getData() == data) {
                    temp = current;
                    prev.setNext(null);

                    loop = false;
                } else if (current.getNext() != null) {
                    if (prev == current) {
                        current = current.getNext();
                    } else {
                        current = current.getNext();
                        prev = prev.getNext();
                    }
                } else {
                    loop = false;
                }
            }

            if (temp != null) {
                System.out.printf("Node Data %s Berhasil Dihapus.\n", data);
                size -= 1;
            } else {
                System.out.println("Data not found.");
            }
        }
    }

    public void print() {
        StringBuilder text = new StringBuilder("Linked List Component = ");
        Node current = head;

        while(current.getNext() != null) {
            text.append(current.getData()).append(" -> ");
            current = current.getNext();
        }
        text.append(current.getData());
        System.out.println(text);
    }

    public void length() {
        System.out.printf("Jumlah Panjang data adalah %s\n", size);
    }
}
