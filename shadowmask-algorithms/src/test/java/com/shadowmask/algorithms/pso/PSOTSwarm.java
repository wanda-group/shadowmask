package com.shadowmask.algorithms.pso;

import java.util.ArrayList;
import java.util.List;

public class PSOTSwarm extends
    Swarm<PSOTVelocity, PSOTFitness, PSOTPosition, PSOTSwarm, PSOTParticle> {

  List<PSOTParticle> particles;

  public PSOTSwarm() {
    init();
  }

  private void  init(){
    particles = new ArrayList<>(particleSize());
    for(int i = 0 ;i<particleSize();++i){
      particles.add(new PSOTParticle(this));
    }
  }

  @Override public List<PSOTParticle> particles() {
    return particles;
  }

  @Override public int maxSteps() {
    return 5000;
  }

  @Override public int particleSize() {
    return 20;
  }

  @Override public void updateCurrentBestParticle(PSOTParticle p) {
    super.updateCurrentBestParticle(p);
//    if(p !=null) {
//      System.out.println(p.currentPosition.xValue+"\t"+p.currentFitness().value);
//    }
  }

  @Override public void updateGlobalBestParticle(PSOTParticle p) {

    super.updateGlobalBestParticle(p);

    if(p !=null) {
      System.out.println(p.historyBestPosition.xValue+"\t"+p.historyBestFitness.value+"\t"+p);
    }
  }
}
