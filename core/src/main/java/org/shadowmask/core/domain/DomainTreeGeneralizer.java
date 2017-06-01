package org.shadowmask.core.domain;

import org.shadowmask.core.mask.rules.generalizer.Generalizer;

public abstract class DomainTreeGeneralizer<T, R, TNODE extends DomainTreeNode<TNODE>>
    implements Generalizer<T, R> {
  @Override public R generalize(T t, int level) {
    TNODE leaf = fixLeaf(t);
    if(leaf == null){
      return convertInputToResult(t);
    }
    for (int i = 0; i < level; ++i) {
      if(leaf.getParent()!=null){
        leaf = leaf.getParent();
      }else {
        break;
      }
    }
    return convertNodeToResult(leaf);
  }


  public abstract TNODE fixLeaf(T t);

  public abstract R convertNodeToResult(TNODE tnode);

  public abstract R convertInputToResult(T t);
}
