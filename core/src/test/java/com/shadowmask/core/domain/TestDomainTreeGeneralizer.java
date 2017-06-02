package com.shadowmask.core.domain;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.shadowmask.core.mask.rules.generalizer.impl.DomainTreeGeneralizer;

public class TestDomainTreeGeneralizer
    extends DomainTreeGeneralizer<String, String, TestTreeNode> {

  public DomainTestTree tree = new DomainTestTree();
  public Map<String, TestTreeNode> nodeMap = new HashMap<>();

  {
    for (TestTreeNode node : tree.getLeaves()) {
      nodeMap.put(node.getName(), node);
    }
  }

  @Override public TestTreeNode fixLeaf(String s) {
    return nodeMap.get(s);
  }

  @Override public String convertNodeToResult(TestTreeNode testTreeNode) {
    return testTreeNode.getName();
  }

  @Override public String convertInputToResult(String s) {
    return s;
  }

  @Test public void test() {
    String res = this.generalize("北京,东城,宣武门",0);
    Assert.assertEquals(res,"北京,东城,宣武门");
    res = this.generalize("北京,东城,宣武门",1);
    Assert.assertEquals(res,"北京,东城");
    res = this.generalize("北京,东城,宣武门",2);
    Assert.assertEquals(res,"北京");
    res = this.generalize("北京,东城,宣武门",3);
    Assert.assertEquals(res,"*");
    res = this.generalize("北京,东城,宣武门",30);
    Assert.assertEquals(res,"*");
    res = this.generalize("北京,东城,宣武门1",2);
    Assert.assertEquals(res,"北京,东城,宣武门1");
  }

  @Override public int getRootLevel() {
    return 10;
  }
}
