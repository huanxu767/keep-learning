package com.xh.keeplearning.jdk.java8.lambda;

/**
 * @author Benjamin Winterberg
 */
public class Interface1 {

    interface MyFormula {
        double calculate(int a);

        default double sqrt(int a) {
            return Math.sqrt(positive(a));
        }

        static int positive(int a) {
            return a > 0 ? a : 0;
        }
    }

    public static void main(String[] args) {
        MyFormula formula = new MyFormula() {
            @Override
            public double calculate(int a) {
                return 0;
            }
        };

        MyFormula formula1 = new MyFormula() {
            @Override
            public double calculate(int a) {
                return sqrt(a * 100);
            }
        };

        formula1.calculate(100);     // 100.0
        formula1.sqrt(-23);          // 0.0
//        Formula.positive(-4);        // 0.0

//        Formula formula2 = (a) -> sqrt( a * 100);
    }

}