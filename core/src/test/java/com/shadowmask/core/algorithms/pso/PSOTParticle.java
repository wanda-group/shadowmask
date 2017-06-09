package com.shadowmask.core.algorithms.pso;

import org.shadowmask.core.algorithms.pso.Particle;

public class PSOTParticle
    implements Particle<PSOTPosition, PSOTVelocity, PSOTFitness> {

  private double lbound = -999999999D;

  private double hbound = 999999999D;

  public PSOTPosition currentPosition;

  public PSOTVelocity currentV;

  public PSOTPosition historyBestPosition;
  public PSOTFitness historyBestFitness;
  public PSOTFitness currentFitness;

  PSOTSwarm swarm;

  public PSOTParticle(PSOTSwarm swarm) {
    this.swarm = swarm;
    init();
  }

  @Override public void init() {
    double position = Math.random() * (hbound - lbound) + lbound;
    this.currentPosition = new PSOTPosition();
    this.currentPosition.xValue = position;
    double randV = Math.random() * (hbound - lbound) + lbound;
    currentV = new PSOTVelocity();
    currentV.velocity = randV;

    PSOTPosition historyBestPos = new PSOTPosition();
    historyBestPos.xValue = position;
    this.historyBestPosition = historyBestPos;
    double fitValue = PSOTTarget.func(this.currentPosition.xValue);
    this.historyBestFitness = new PSOTFitness();
    this.historyBestFitness.value = fitValue;
    this.currentFitness = new PSOTFitness();
    this.currentFitness.value = fitValue;
  }

  @Override public void move(PSOTVelocity newVelocity) {
    this.currentPosition.xValue =
        this.currentPosition.xValue + newVelocity.velocity;
    if (currentPosition.xValue < lbound) {
      currentPosition.xValue = lbound;
    } else if (currentPosition.xValue > hbound) {
      currentPosition.xValue = hbound;
    }
  }

  @Override public PSOTPosition currentPosition() {
    return currentPosition;
  }

  @Override public PSOTFitness currentFitness() {
    return currentFitness;
  }

  @Override public PSOTVelocity currentVelocity() {
    return currentV;
  }

  @Override public PSOTPosition historyBestPosition() {
    return historyBestPosition;
  }

  @Override public PSOTFitness historyBestFitness() {
    return historyBestFitness;
  }

  @Override public void updateVelocity(PSOTVelocity velocity) {
    this.currentV = velocity;
  }

  @Override public void getBetter(PSOTPosition betterPosition,
      PSOTFitness betterFitness) {
    this.historyBestFitness.value = betterFitness.value;
    this.historyBestPosition.xValue = betterPosition.xValue;
  }
}
