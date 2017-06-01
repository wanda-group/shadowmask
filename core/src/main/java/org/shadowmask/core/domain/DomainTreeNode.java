package org.shadowmask.core.domain;

import java.util.List;

/**
 * domain tree node
 */
public class DomainTreeNode<N extends DomainTreeNode> {

  private String name;

  private N parent;

  private List<N> children;

  private int depth;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public N getParent() {
    return parent;
  }

  public void setParent(N parent) {
    this.parent = parent;
  }

  public List<N> getChildren() {
    return children;
  }

  public void setChildren(List<N> children) {
    this.children = children;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }
}
