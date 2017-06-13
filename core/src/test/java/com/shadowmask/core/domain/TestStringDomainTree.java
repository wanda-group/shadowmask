package com.shadowmask.core.domain;

import org.junit.Assert;
import org.junit.Test;
import org.shadowmask.core.domain.tree.StringTaxTree;

public class TestStringDomainTree {

  @Test
  public void testTree(){
    StringTaxTree tree = new StringTaxTree("{\"comparable\":false,\"type\":\"string\",\"version\":1.0,\"root\":{\"text\":\"中国\",\"children\":[{\"text\":\"上海\",\"children\":[{\"text\":\"上海,闵行\",\"children\":[{\"text\":\"上海,闵行,浦江镇\"},{\"text\":\"上海,闵行,三林\"}]},{\"text\":\"上海,浦东\",\"children\":[{\"text\":\"上海,浦东,塘桥\"}]}]},{\"text\":\"北京\",\"children\":[{\"text\":\"北京东城\",\"children\":[{\"text\":\"北京,东城,西直门\"},{\"text\":\"北京,东城,宣武门\"}]}]}]}}\n");

    Assert.assertEquals(tree.getLeaves().size(),5);
  }

}
