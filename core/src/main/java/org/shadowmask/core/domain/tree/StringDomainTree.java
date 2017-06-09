package org.shadowmask.core.domain.tree;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.shadowmask.core.domain.tree.StringDomainTree.StringDomainTreeNode;
import org.shadowmask.core.util.JsonUtil;

/**
 * String domain tree
 */

public class StringDomainTree extends DomainTree<StringDomainTreeNode> implements LeafLocator<String,StringDomainTreeNode> {
  public StringDomainTree(String json) {
    super(json);
  }

  public StringDomainTree() {
    super();
  }

  /**
   * hash map index
   */
  private Map<String, StringDomainTreeNode> index;

  @Override protected StringDomainTreeNode constructTNode(String jsonStr) {
    Gson gson = JsonUtil.newGsonInstance();
    JsonObject object = gson.fromJson(jsonStr, JsonObject.class);
    String text = object.get("text").getAsString();
    StringDomainTreeNode node = new StringDomainTreeNode();
    node.setName(text);
    return node;
  }

  @Override public void onTreeBuilt() {
    index = new HashMap<>();
    for (StringDomainTreeNode node : this.getLeaves()) {
      index.put(node.getName(), node);
    }
  }

  public StringDomainTreeNode fixALeaf(String name) {
    return this.index.get(name);
  }

  @Override public StringDomainTreeNode locate(String s) {
    return null;
  }

  static class StringDomainTreeNode
      extends DomainTreeNode<StringDomainTreeNode> {

  }

}

