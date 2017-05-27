package com.shadowmask.algorithms.pso;

public interface Particle<P extends Position, V extends Velocity, F extends Fitness, SWARM extends Swarm> {

  /**
   * init operation .
   */
  public void init();


  void move();

  /**
   * swarm
   */
  SWARM swarmBelonged();

  /**
   * calculate a new velocity.
   * @return
   */
  V newVelocity();

  /**
   * current position
   */
  P currentPosition();

  /**
   * current fitness
   */
  F currentFitness();

  /**
   * current velocity
   */
  V currentVelocity();

  /**
   * best position since beginning.
   */
  P historyBestPosition();

  /**
   * best fitness since beginning.
   */
  F historyBestFitness();

}
