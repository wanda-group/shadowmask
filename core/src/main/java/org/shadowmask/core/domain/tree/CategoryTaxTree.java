package org.shadowmask.core.domain.tree;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.shadowmask.core.data.DataType;
import org.shadowmask.core.domain.TaxTreeType;
import org.shadowmask.core.domain.tree.CategoryTaxTree.CategoryTaxTreeNode;
import org.shadowmask.core.util.JsonUtil;

/**
 * String domain tree
 */

public class CategoryTaxTree extends TaxTree<CategoryTaxTreeNode>
    implements LeafLocator<String> {
  /**
   * hash map index
   */
  private Map<String, CategoryTaxTreeNode> index;

  public CategoryTaxTree(String json) {
    super(json);
  }

  public CategoryTaxTree() {
    super();
  }

  @Override public TaxTreeType type() {
    return TaxTreeType.CATEGORY;
  }

  @Override public DataType dataType() {
    return DataType.STRING;
  }

  @Override protected CategoryTaxTreeNode constructTNode(String jsonStr) {
    Gson gson = JsonUtil.newGsonInstance();
    JsonObject object = gson.fromJson(jsonStr, JsonObject.class);
    String text = object.get("text").getAsString();
    CategoryTaxTreeNode node = new CategoryTaxTreeNode();
    node.setName(text);
    return node;
  }

  @Override public void onTreeBuilt() {
    index = new HashMap<>();
    for (CategoryTaxTreeNode node : this.getLeaves()) {
      index.put(node.getName(), node);
    }
  }

  public CategoryTaxTreeNode fixALeaf(String name) {
    return this.index.get(name);
  }

  @Override public CategoryTaxTreeNode locate(String s) {
    return this.fixALeaf(s);
  }

  public static class CategoryTaxTreeNode extends TaxTreeNode {

  }

}

