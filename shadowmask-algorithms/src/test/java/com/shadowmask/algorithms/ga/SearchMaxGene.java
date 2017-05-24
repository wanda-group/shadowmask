package com.shadowmask.algorithms.ga;

import java.util.Random;
import org.javatuples.Pair;

public class SearchMaxGene implements Gene<SearchMaxGene> {

  public String binStr;

  public SearchMaxGene(String binStr) {

    int dLen = 30 - binStr.length();
    StringBuilder prefix = new StringBuilder();
    for (int i = 0; i < dLen ; i++) {
      prefix.append("0");
    }

    this.binStr = prefix.toString()+binStr;
  }

  @Override
  public Pair<SearchMaxGene, SearchMaxGene> cross(SearchMaxGene that) {
    int index = new Random().nextInt(this.binStr.length());
    String pre1 = this.binStr.substring(0, index);
    String post1 = this.binStr.substring(index);
    String pre2 = that.binStr.substring(0, index);
    String post2 = that.binStr.substring(index);
    String gene1 = pre1 + post2;
    String gene2 = pre2 + post1;
    return new Pair<>(new SearchMaxGene(gene1), new SearchMaxGene(gene2));
  }

  @Override
  public SearchMaxGene mutate() {

    int mutateNum = new Random().nextInt(binStr.length());
    StringBuilder builder = new StringBuilder(binStr);
    for(int i = 0 ; i< mutateNum; ++ i){
      int index = new Random().nextInt(binStr.length());
      char g = binStr.charAt(index);
      if ('0' == g) {
        builder.setCharAt(index, '1');
      } else {
        builder.setCharAt(index, '0');
      }
    }

    return new SearchMaxGene(builder.toString());
  }
}
