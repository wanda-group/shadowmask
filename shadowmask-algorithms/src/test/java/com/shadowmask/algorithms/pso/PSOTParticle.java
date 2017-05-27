package com.shadowmask.algorithms.pso;

public class PSOTParticle
    implements Particle<PSOTPosition, PSOTVelocity, PSOTFitness, PSOTSwarm> {

  private double lbound = -10000D;

  private double hbound = 10000D;

  private double randomSearchRate = 0.01;

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

  @Override public void move() {
    PSOTVelocity newVelocity = newVelocity();
    // update position
    this.currentPosition.xValue =
        this.currentPosition.xValue + newVelocity.velocity;
    if (currentPosition.xValue < lbound) {
      currentPosition.xValue = lbound;
    } else if (currentPosition.xValue > hbound) {
      currentPosition.xValue = hbound;
    }

    double currentValue = PSOTTarget.func(currentPosition.xValue);

    currentFitness.value = currentValue;
    //update current fitness
    if (currentFitness.betterThan(historyBestFitness())) {
      // update best fitness
      historyBestFitness.value = currentFitness.value;
      // update best position
      historyBestPosition.xValue = currentPosition.xValue;
    }
  }

  @Override public PSOTVelocity newVelocity() {
    double rate = Math.random();
    if (rate < randomSearchRate) {
      double value = Math.random() * (hbound - lbound);
      PSOTVelocity v = new PSOTVelocity();
      v.velocity = value;
      return v;
    }
    PSOTSwarm swarm = this.swarmBelonged();
    PSOTParticle worstPA = swarm.currentWorstParticle();
    PSOTParticle bestPA = swarm.currentBestParticle();
    PSOTParticle globalBestParticle = swarm.globalBestParticle();

    double divider =
        bestPA.currentFitness().value - worstPA.currentFitness.value;

    double stayRate = 0;
    if (divider * 1000000 != 0) {
      stayRate = (this.currentFitness.value - worstPA.currentFitness().value)
          / divider;
    }
    divider =
        globalBestParticle.historyBestFitness.value - this.currentFitness.value;


    double selfLearnRate = 1;

    if (divider * 1000000 != 0) {
      selfLearnRate =
          (this.historyBestFitness().value - this.currentFitness.value) / divider;
    }
    double learnOther = 1 - selfLearnRate;
    double v =
        this.currentV.velocity * stayRate + selfLearnRate * Math.random() * (
            this.historyBestPosition.xValue - this.currentPosition.xValue)
            + learnOther * Math.random() * (
            swarm.globalBestParticle().historyBestPosition.xValue
                - this.currentPosition.xValue);
    PSOTVelocity vv = new PSOTVelocity();
    vv.velocity = v;
    return vv;
  }

  @Override public PSOTSwarm swarmBelonged() {
    return swarm;
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
}
