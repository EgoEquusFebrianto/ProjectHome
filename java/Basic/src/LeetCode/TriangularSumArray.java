package LeetCode;

public class TriangularSumArray {
    public static int[] Solution(int[] nums) {
        int steps = nums.length;
        for (var step = 1; step < steps; step++) {
            for (var i = 0; i <= steps - step - 1; i++) {
                nums[i] = (nums[i] + nums[i+1]) % 10;
            }
        }
        return nums;
    }

    public static void main(String[] args) {
        int[] nums = {1,2,3,4,5};
        int[] solution = Solution(nums);

        for (var i : solution) {
            System.out.printf("%d, ", i);
        }
    }
}
