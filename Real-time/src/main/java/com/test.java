package com;

/**
 * @BelongsProject: test-git
 * @BelongsPackage: com
 * @Author: liwenjie
 * @CreateTime: 2025-08-26  22:36
 * @Description: TODO
 * @Version: 1.0
 */
import java.util.HashMap;
import java.util.Map;

public class test {
    public static int[] twoSum(int[] nums, int target) {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length; i++) {
            int complement = target - nums[i];
            if (map.containsKey(complement)) {
                return new int[] { map.get(complement), i };
            }
            map.put(nums[i], i);
        }
        // 如果没有找到符合条件的结果，抛出异常
        throw new IllegalArgumentException("No two sum solution");
    }

    public static void main(String[] args) {
        // 设置nums数组和target值
        int[] nums = {2, 4, 6, 8};
        int target = 12;

        // 调用函数并获取结果
        int[] result = twoSum(nums, target);

        // 打印结果
        System.out.println("两数之和为" + target + "的下标是: [" + result[0] + ", " + result[1] + "]");
        // 输出: 两数之和为12的下标是: [1, 3] （4+8=12）
    }
}