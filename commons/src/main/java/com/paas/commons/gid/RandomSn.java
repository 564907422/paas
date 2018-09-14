package com.paas.commons.gid;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 17/11/23.
 */
public class RandomSn {
    private static String uppers = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    /**
     * 获得nums位随机大写字母
     * 可以重复出现
     * @param nums
     * @return
     */
    public static String nextUppers(int nums){
        String randomcode2 = "";
        char[] m = uppers.toCharArray();

        for (int j=0;j<nums ;j++ )
        {
            char c = m[(int)(Math.random()*26)];
            randomcode2 = randomcode2 + c;
        }
        return randomcode2;
    }

    /**
     * yyyyMMddHHmmssSSSxxx  毫秒+n位随机数
     * @param nums
     * @return
     */
    public static String nextMsSn(int nums) {
        String ms = msFormat.format(new Date());
        if(nums<1)
            return ms;
        return ms+generateRandomNumber(nums);
    }
    /**
     * 产生n位随机数
     * @return
     */
    public static long generateRandomNumber(int n){
        if(n<1){
            throw new IllegalArgumentException("随机数位数必须大于0");
        }
        return (long)(Math.random()*9*Math.pow(10,n-1)) + (long)Math.pow(10,n-1);
    }

    public static void main(String[] args){
        for(int i=0;i<5000;i++){
            System.out.println(nextMsSn(4));
        }
    }
}
