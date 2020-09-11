package com.xh.keeplearning;

import java.util.Stack;
import java.util.Stack;	//引用栈

/**
 * https://juejin.im/post/5bce68226fb9a05ce46a0476
 * java 值传递 与 引用传递
 */
public class D {
    public static void main(String[] args) {
        //实参
        int a = 10;
        func(a);
        System.out.println(a);
        System.out.println("-------");

        //行参
        int a1 = 0;
        func(a1);
        System.out.println(a1);
        System.out.println("-------");
        //行参
        String s = null;
        func(s);
        System.out.println(s);
        System.out.println("-------");
        //行参
        String s2 = "111";
        func(s2);
        System.out.println(s2);

        Stack<Integer> stack = new Stack<>();
        for (int i = 0; i < 5; i++) {
            stack.push(i);
        }
        while (!stack.empty()){
            System.out.println(stack.pop());
        }


    }
    public static void func(int a){
        a=20;
        System.out.println(a);
    }

    public static void func(String a){
        a = "hello";
        System.out.println(a);
    }
}
