package com.asml.api;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JuliaJNIlib {



  /*
  public static void main(String[] args) throws IOException {
    String dirName = "/home/janbodnar/prog/";
    Path currentRelativePath = Paths.get("");
    String s = currentRelativePath.toAbsolutePath().toString();
    Files.list(new File(s).toPath())
            .limit(15)
            .forEach(path -> {
              System.out.println(path);
            });
    generalTest();
    System.out.println("-----------");
    jni_multithread();
  }


   */

 public static int callDouble(int value, String path){
   loadLibrary(path);
   Path currentRelativePath = Paths.get("");
   String s = currentRelativePath.toAbsolutePath().toString();
   System.out.println("Current absolute path is: " + s);
   System.out.println("Current so is " + s+"/libDoubleInt.so");
   initJulia(path, "/libDoubleInt.so");
   var res = doubleIt(value);
   System.out.println("double val: "+ res);
   endJulia();
   return res;
 }

 public static void end(){
   endJulia();
 }
  public static void loadLibrary(String path) {
    Path currentRelativePath = Paths.get("");
    String s = currentRelativePath.toAbsolutePath().toString();
    System.out.println("Current absolute path is: " + s);
    System.load(path + "/libTrivialFunctions.so");
  }

  private static native void initJulia(String path, String libName);

  private static native void endJulia();

  private static native int doubleIt(int a);

  private static native boolean isEven(int a);

  private static native double[] regressionLine(double[] x, double[] y);

  private static native MyAverage reduce(MyAverage buffer, double data);

  private static native MyAverage merge(MyAverage bufferA, MyAverage bufferB);

  private static native double finish(MyAverage buffer);

  public static void generalTest(String path) {
    loadLibrary("");
    Path currentRelativePath = Paths.get("");
    String s = currentRelativePath.toAbsolutePath().toString();
    System.out.println("Current absolute path is: " + s);
    initJulia(s, "/libDoubleInt.so");
    //or retrieve it from using pldd: initJulia("", "libDoubleInt.so");
    //the pldd is not allowed on Ubuntu WSL
    System.out.println(" x = " + doubleIt(8));
    if (isEven(8)) {
      System.out.println(" Number is Even");
    } else {
      System.out.println(" Number is Odd");
    }

    System.out.println("Running the regression");
    double[] x_array = { 1.1, 2.2, 3.3, 4.4, 5.5, 6.6 };
    double[] y_array = { 11.1, 22.2, 33.3, 44.4, 55.5, 66.6 };

    double[] res = regressionLine(x_array, y_array);
    System.out.println("a = " + res[0] + " b = " + res[1]);

    MyAverage my_avg1 = new MyAverage(20.48, 23);
    System.out.println(my_avg1.toString());
    my_avg1 = reduce(my_avg1, 23.45);
    System.out.println("After the reduce: " + my_avg1.toString());
    MyAverage my_avg2 = new MyAverage(120.1248, 45);
    MyAverage my_avg3 = merge(my_avg1, my_avg2);
    System.out.println("After the merge: " + my_avg3.toString());
    System.out.println("Finish the merge: " + finish(my_avg3));
    endJulia();
  }

  public static void jni_multithread(String path) {
    loadLibrary(path);
    initJulia("/tmp/workspace/java/src", "libDoubleInt.so");

    var mainThread = Thread.currentThread().getName();
    System.out.println("Thread: " + mainThread + " running" + " x = " + doubleIt(8));
    System.out.println("Thread: " + mainThread + " running" + " x = " + doubleIt(1));

    Thread[] threads = new Thread[10];
    for(int i=0; i<10; i++){
      Thread t = new Thread("" + i) {
        public void run(){
          initJulia("/tmp/workspace/java/src", "libDoubleInt.so");
          var doubleItResult = doubleIt(Integer.valueOf(getName()));
          System.out.println("Thread: " + getName() + " running" + " doubleIt(x) = " + doubleItResult);
          var isEvenResult = isEven(Integer.valueOf(getName()));
          System.out.println("Thread: " + getName() + " running" + " isEven(x) = " + isEvenResult);
        }
      };
      t.start();
      threads[i] = t;
    }
    for(int i=0; i<10; i++){
      try {
        threads[i].join();
      } catch (Exception e) {
        System.out.println("error " + e);
      }
    }
    System.out.println("terminate");
    endJulia();
  }

  public static class MyAverage {

    private double sum = 0;
    private long count = 0;

    public MyAverage() {}

    public MyAverage(double sum, long count) {
      this.sum = sum;
      this.count = count;
    }

    public double getSum() {
      return sum;
    }

    public long getCount() {
      return count;
    }

    @Override
    public String toString() {
      return "MyAverage{sum=" + sum + ", count=" + count + "}";
    }
  }
}