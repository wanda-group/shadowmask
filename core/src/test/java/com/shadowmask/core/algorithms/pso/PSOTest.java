package com.shadowmask.core.algorithms.pso;

import org.jfree.chart.renderer.xy.XYSplineRenderer;
import org.junit.Test;

public class PSOTest {

  @Test public void test() {
    PSOTSwarm swarm = new PSOTSwarm();
    swarm.optimize();
  }

  @Test public void testPlot() {
    XYSplineRenderer renderer = new XYSplineRenderer();
    Long l = 1l;
    double d = 0.5F;
  }
}
