package org.shadowmask.core.domain.tree;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.shadowmask.core.domain.tree.StringTaxTree.StringTaxTreeNode;
import org.shadowmask.core.util.JsonUtil;

/**
 * String domain tree
 */

public class StringTaxTree extends TaxTree<StringTaxTreeNode> implements LeafLocator<String> {
  public StringTaxTree(String json) {
    super(json);
  }

  public StringTaxTree() {
    super();
  }

  /**
   * hash map index
   */
  private Map<String, StringTaxTreeNode> index;

  @Override protected StringTaxTreeNode constructTNode(String jsonStr) {
    Gson gson = JsonUtil.newGsonInstance();
    JsonObject object = gson.fromJson(jsonStr, JsonObject.class);
    String text = object.get("text").getAsString();
    StringTaxTreeNode node = new StringTaxTreeNode();
    node.setName(text);
    return node;
  }

  @Override public void onTreeBuilt() {
    index = new HashMap<>();
    for (StringTaxTreeNode node : this.getLeaves()) {
      index.put(node.getName(), node);
    }
  }

  public StringTaxTreeNode fixALeaf(String name) {
    return this.index.get(name);
  }

  @Override public StringTaxTreeNode locate(String s) {
    return null;
  }

  static class StringTaxTreeNode
      extends TaxTreeNode {

  }

}

