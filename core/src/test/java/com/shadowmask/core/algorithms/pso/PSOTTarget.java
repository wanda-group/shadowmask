package com.shadowmask.core.algorithms.pso;

public class PSOTTarget {

  public static double func(double xValue){
//    return xValue*xValue;

//    if(xValue<100 && xValue>99){
//      return -100D+(100-xValue);
//    }

//    return -Math.sin(xValue-999);
    return -Math.sin(xValue)*1000/(xValue);

  }
}
