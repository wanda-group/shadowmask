package com.shadowmask.algorithms.ga;

/**
 * gene encoded to chromosome , chromosome decoded to chromosome .
 */
public interface GeneCoder<CH extends Chromosome, GENE extends Gene> {

  /**
   * encode a chromosome to a gene .
   */
  public GENE encode(CH chromosome);

  /**
   * decode a gene to chromosome .
   */
  public CH decode(GENE gene);
}
