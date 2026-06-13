package LeetCode;

import java.util.HashMap;
import java.util.Map;

public class Problem1_TwoSum {
    public static int[] twoSum(int[] nums, int target) {
        Map<Integer, Integer> numMap = new HashMap<>();
        int n = nums.length;

        for (int i = 0; i < n; i++) {
            numMap.put(nums[i], i);
        }

        for(int i = 0; i < n; i++) {
            int complement = target - nums[i];
            if (numMap.containsKey(complement) && numMap.get(complement) != i) {
                return new int[]{i, numMap.get(complement)};
            }
        }

        return new int[]{};
    }

    public static void main(String[] args) {
//        int[] nums = {2, 7, 11, 13};
//        int target = 15;
//
//        int[] result = twoSum(nums, target);
//
//        for (int i : result) {
//            System.out.printf("%d, ", i);
//        }
        int carry = 7 / 10;
        System.out.println(carry);
    }
}