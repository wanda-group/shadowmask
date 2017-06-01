package org.shadowmask.core.domain;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.shadowmask.core.domain.StringDomainTree.StringDomainTreeNode;
import org.shadowmask.core.util.JsonUtil;

/**
 * String domain tree
 */

public class StringDomainTree extends DomainTree<StringDomainTreeNode> {
  public StringDomainTree(String json) {
    super(json);
  }

  public StringDomainTree() {
    super();
  }

  @Override protected StringDomainTreeNode constructTNode(String jsonStr) {
    Gson gson = JsonUtil.newGsonInstance();
    JsonObject object = gson.fromJson(jsonStr, JsonObject.class);
    String text = object.get("text").getAsString();
    StringDomainTreeNode node = new StringDomainTreeNode();
    node.setName(text);
    return node;
  }

  static class StringDomainTreeNode
      extends DomainTreeNode<StringDomainTreeNode> {

  }

}

