class Node:
    def __init__(self, x):
        self.data: any = x
        self.next: Node = None
    
class LinkedList:
    def __init__(self):
        self.head: Node = None
        self.size: int = 0
    
    def append(self, data):
        newNode: Node = Node(data)

        if (self.head == None):
            self.head = newNode
        else:
            current = self.head
            while current.next != None:
                current = current.next
            current.next = newNode
        self.size += 1
    
    def insert(self, data):
        newNode: Node = Node(data)
        if self.head == None:
            self.head = newNode
        else:
            newNode.next = self.head
            self.head = newNode
        self.size += 1
    
    def deleteHead(self):
        if self.head != None:
            temp = self.head
            self.head = temp.next
            self.size -= 1
            print(temp.data)
        else:
            print("Delete Error, No data in LinkedList")
    
    def deleteTail(self):
        if self.head.next != None:
            if self.head.next == None:
                self.head = None
            else:
                temp = self.head
                while temp.next.next != None:
                    temp = temp.next
                lastNode: Node = temp.next
                temp.next = None
                print(lastNode.data)
                lastNode = None
                self.size -= 1
    
    def deleteNode(self, data):
        if self.head == None:
            print("Linked List belum memiliki data.")
        elif self.head.next == None:
            if self.head.data == data:
                self.head = None
            else:
                print("Node Tidak ditemukan.")
        else:
            prevNode: Node = self.head
            current: Node = self.head
            temp: Node = None

            loop = True
            while loop:
                if current.data == data and current.next:
                    temp = current.next
                    loop = False
                elif current.data == data:
                    temp = current
                    loop = False
                elif current.next != None:
                    if prevNode == current:
                        current = current.next
                    else:
                        prevNode = prevNode.next
                        current = current.next
                else:
                    loop = False

            if temp and current == prevNode:
                self.head = temp
                print(f"Node Data {data} Berhasil Dihapus.")
            elif temp == current:
                prevNode.next = None
                print(f"Node Data {data} Berhasil Dihapus.")
            elif temp:
                prevNode.next = temp
                print(f"Node Data {data} Berhasil Dihapus.")
            else:
                print("Node Tidak Ditemukan.")

    def find(self, data):
        current = self.head
        pos = 0
        while pos < self.size:
            if current.data == data:
                print(f"Position of {data} is {pos}")
                break
            else:
                current = current.next
                post += 1
        if pos == self.size:
            print("Data not found.")

    def print(self):
        text = "Linked List ="
        if self.head == None:
            print("Linked List belum terdapat data tersimpan.")
        elif self.head.next == None:
            print(text + self.head.data)
        else:
            current = self.head
            while current.next:
                text += f" {current.data} ->"
                current = current.next
            text += f" {current.data}"
            print(text)
    
data = LinkedList()
data.append(1)
data.append(2)
data.append(3)
data.append(4)
data.append(5)
# data.print()

data.insert(0)
data.print()

data.deleteNode(0)
data.print()

data.deleteNode(2)
data.print()

data.deleteNode(5)
data.print()

data.deleteNode(6)
data.print()