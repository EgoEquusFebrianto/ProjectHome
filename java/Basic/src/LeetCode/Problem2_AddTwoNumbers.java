package LeetCode;

import LeetCode.lib.ListNode;

public class Problem2_AddTwoNumbers {
    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode dummyHead = new ListNode(0);
        ListNode tail = dummyHead;
        int carry = 0;

        while (l1 != null || l2 != null || carry != 0) {
            int digit1 = (l1 != null) ? l1.val : 0;
            int digit2 = (l2 != null) ? l2.val : 0;

            int sum = digit1 + digit2 + carry;
            int digit = sum % 10;
            carry = sum / 10;

            ListNode newNode = new ListNode(digit);
            tail.next = newNode;
            tail = tail.next;

            l1 = (l1 != null) ? l1.next : null;
            l2 = (l2 != null) ? l2.next : null;
        }

        ListNode result = dummyHead.next;
        dummyHead.next = null;
        return result;
    }

    // Helper method: Membuat linked list dari array
    private static ListNode createList(int[] arr) {
        if (arr.length == 0) return null;
        ListNode head = new ListNode(arr[0]);
        ListNode current = head;
        for (int i = 1; i < arr.length; i++) {
            current.next = new ListNode(arr[i]);
            current = current.next;
        }
        return head;
    }

    // Helper method: Mencetak linked list
    private static void printList(ListNode head, String message) {
        System.out.print(message + ": ");
        ListNode current = head;
        while (current != null) {
            System.out.print(current.val);
            if (current.next != null) System.out.print(" → ");
            current = current.next;
        }
        System.out.println();
    }

    // Helper method: Menghitung nilai angka dari linked list (untuk verifikasi)
    private static long getNumber(ListNode head) {
        long number = 0;
        long multiplier = 1;
        ListNode current = head;
        while (current != null) {
            number += current.val * multiplier;
            multiplier *= 10;
            current = current.next;
        }
        return number;
    }

    public static void main(String[] args) {

        // Test Case 1: l1 = [2,4,3], l2 = [5,6,4]
        System.out.println("========== TEST CASE 1 ==========");
        int[] arr1_1 = {2, 4, 3};  // angka 342
        int[] arr2_1 = {5, 6, 4};  // angka 465

        ListNode l1_1 = createList(arr1_1);
        ListNode l2_1 = createList(arr2_1);

        printList(l1_1, "l1 (342)");
        printList(l2_1, "l2 (465)");

        ListNode result1 = addTwoNumbers(l1_1, l2_1);
        printList(result1, "Hasil");
        System.out.println("Verifikasi: " + getNumber(l1_1) + " + " + getNumber(l2_1) + " = " + getNumber(result1));
        System.out.println();

        // Test Case 2: l1 = [9,9,9,9,9,9,9], l2 = [9,9,9,9]
        System.out.println("========== TEST CASE 2 ==========");
        int[] arr1_2 = {9, 9, 9, 9, 9, 9, 9};  // angka 9.999.999
        int[] arr2_2 = {9, 9, 9, 9};            // angka 9.999

        ListNode l1_2 = createList(arr1_2);
        ListNode l2_2 = createList(arr2_2);

        printList(l1_2, "l1 (9.999.999)");
        printList(l2_2, "l2 (9.999)");

        ListNode result2 = addTwoNumbers(l1_2, l2_2);
        printList(result2, "Hasil");
        System.out.println("Verifikasi: " + getNumber(l1_2) + " + " + getNumber(l2_2) + " = " + getNumber(result2));
        System.out.println();

        // Test Case 3: l1 = [0], l2 = [0]
        System.out.println("========== TEST CASE 3 ==========");
        int[] arr1_3 = {0};
        int[] arr2_3 = {0};

        ListNode l1_3 = createList(arr1_3);
        ListNode l2_3 = createList(arr2_3);

        printList(l1_3, "l1");
        printList(l2_3, "l2");

        ListNode result3 = addTwoNumbers(l1_3, l2_3);
        printList(result3, "Hasil");
        System.out.println("Verifikasi: " + getNumber(l1_3) + " + " + getNumber(l2_3) + " = " + getNumber(result3));
        System.out.println();

        // Test Case 4: l1 = [5], l2 = [5] (menghasilkan carry)
        System.out.println("========== TEST CASE 4 ==========");
        int[] arr1_4 = {5};
        int[] arr2_4 = {5};

        ListNode l1_4 = createList(arr1_4);
        ListNode l2_4 = createList(arr2_4);

        printList(l1_4, "l1");
        printList(l2_4, "l2");

        ListNode result4 = addTwoNumbers(l1_4, l2_4);
        printList(result4, "Hasil");
        System.out.println("Verifikasi: " + getNumber(l1_4) + " + " + getNumber(l2_4) + " = " + getNumber(result4));
        System.out.println();

        // Test Case 5: l1 = [1,8], l2 = [0] (panjang berbeda)
        System.out.println("========== TEST CASE 5 ==========");
        int[] arr1_5 = {1, 8};  // angka 81
        int[] arr2_5 = {0};      // angka 0

        ListNode l1_5 = createList(arr1_5);
        ListNode l2_5 = createList(arr2_5);

        printList(l1_5, "l1 (81)");
        printList(l2_5, "l2 (0)");

        ListNode result5 = addTwoNumbers(l1_5, l2_5);
        printList(result5, "Hasil");
        System.out.println("Verifikasi: " + getNumber(l1_5) + " + " + getNumber(l2_5) + " = " + getNumber(result5));
    }
}
