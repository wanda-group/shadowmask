package com.shadowmask.algorithms.pso;

public class PSOTTarget {

  public static double func(double xValue){
//    return xValue*xValue;

    return -Math.sin(xValue)*10/(xValue);

  }
}
