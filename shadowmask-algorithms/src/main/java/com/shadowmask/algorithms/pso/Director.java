package com.shadowmask.algorithms.pso;

public interface Director<F extends Fitness, V extends Velocity, P extends Position, PA extends Particle<P, V, F>, SWARM extends Swarm<V, F, P, PA>> {
  /**
   * calculate a velocity according to particle
   * @param p
   * @param swarm
   * @return
   */
  V direct(PA p,SWARM swarm);
}
