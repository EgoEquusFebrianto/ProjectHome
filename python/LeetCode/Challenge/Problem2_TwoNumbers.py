class Node:
    def __init__(self, val):
        self.val: any = val
        self.next: Node = None

    def setNext(self, val):
        self.next = val


class Solution:
    def twoNumbers(self, l1: Node, l2: Node) -> tuple[Node, Node, Node]:
        dummyHead: Node = Node(0)
        tail: Node = dummyHead
        carry = 0

        while l1 is not None or l2 is not None or carry != 0:
            digit1 = l1.val if l1 is not None else 0
            digit2 = l2.val if l2 is not None else 0

            sum = digit1 + digit2 + carry
            digit = sum % 10
            carry = sum // 10

            newNode = Node(digit)
            tail.next = newNode
            tail = tail.next

            l1 = l1.next if l1 is not None else None
            l2 = l2.next if l2 is not None else None

        result = dummyHead.next
        dummyHead.Next = None
        return result, dummyHead, tail

l1 = Node(2)
l2 = Node(5)

l1.setNext(Node(4))
l1.next.setNext(Node(3))

l2.setNext(Node(6))
l2.next.setNext(Node(4))


res, dummyHead, tail = Solution().twoNumbers(l1, l2)

# print(f"{dummyHead.val}-{dummyHead.next.val}-{dummyHead.next.next.val}")
# print(f"{res.val}-{res.next.val}-{res.next.next.val}")
# print(f"{tail.val}-{tail.next}")