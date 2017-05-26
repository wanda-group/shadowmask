package com.shadowmask.algorithms.pso;

import java.util.List;

public abstract class Swarm<V extends Velocity, F extends Fitness, P extends Position, PA extends Particle> {

  /**
   * all particles
   */
  public abstract List<P> particles();

  /**
   * best particle till now .
   */
  public abstract PA globalBestParticle();

  /**
   * best particle in current swarm
   */
  public abstract PA currentBestParticle();

  /**
   * worst particle in current swarm
   */
  public abstract PA currentWorstParticle();

  public void optimize() {

  }

}
