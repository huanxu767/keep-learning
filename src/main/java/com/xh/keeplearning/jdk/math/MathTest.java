package com.xh.keeplearning.jdk.math;

/**
 *
 */
public class MathTest {
    public static void main(String[] args) {
        System.out.println(Math.PI);
        System.out.println(Math.E);

        //三角函数 反三角函数
        System.out.println(Math.sin(Math.PI / 2));
        System.out.println(Math.sin(0));
        System.out.println(Math.toRadians(180.0));
        //e的n次方
        System.out.println(Math.exp(0));
        //对数 log自然对数
        System.out.println("log:" + Math.log(Math.E));
        //y = log(x+1)
        System.out.println("log1p:" + Math.log1p(0));
        System.out.println("log1p:" + Math.log1p(Math.E - 1));
        System.out.println("Math.pow(2,3)" + Math.pow(2, 3));

        //以10为底对数
        System.out.println("log10:" + Math.log10(100));
        //绝对值
        System.out.println("绝对值：" + Math.abs(-2332) + "  " + Math.abs(2332));
        System.out.println("max：" + Math.max(3, 7));
        System.out.println("addExact：" + Math.addExact(3, 5));
        System.out.println("addExact：" + Math.addExact(0, 0));
        //开平方根
        System.out.println("Math.sqrt" + Math.sqrt(9));
        //开立方根
        System.out.println("Math.cbrt" + Math.cbrt(27));
        //异或 相同则结果为0，不同则结果为1
        System.out.println("异或 相同则结果为0，不同则结果为1");
        System.out.println("2 ^ 3：" + (2 ^ 3));
        System.out.println("3 ^ 3：" + (2 ^ 3));

        System.out.println("Math.decrementExact" + Math.decrementExact(10));


    }
}
