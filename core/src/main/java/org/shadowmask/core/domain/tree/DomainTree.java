package org.shadowmask.core.domain.tree;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.shadowmask.core.util.JsonUtil;
import org.yaml.snakeyaml.Yaml;

public abstract class DomainTree<TNODE extends DomainTreeNode> {
  private int height;
  private TNODE root;
  private List<TNODE> leaves;
  private String version;

  public DomainTree(String json) {
    constructFromJson(json);
  }

  public DomainTree() {
  }

  protected abstract TNODE constructTNode(String jsonStr);

  public void constructFromYaml(String yamlStr) {
    Yaml yaml = new Yaml();
    Gson gson = JsonUtil.newGsonInstance();
    String json = gson.toJson(yaml.load(yamlStr));
    this.constructFromJson(json);
  }

  public void constructFromYamlInputStream(InputStream inputStream) {
    Yaml yaml = new Yaml();
    Gson gson = JsonUtil.newGsonInstance();
    String json = gson.toJson(yaml.load(inputStream));
    this.constructFromJson(json);
  }

  public void constructFromJson(String jsonStr) {
    Gson gson = new Gson();
    JsonObject object = gson.fromJson(jsonStr, JsonObject.class);
    String version = object.get("version").toString();
    this.version = version;
    List<TNODE> leaves = new ArrayList<TNODE>();
    int depth = 0;
    TNODE root = constructTree(object.get("root").toString(), leaves, depth);
    this.root = root;
    this.leaves = leaves;
    onTreeBuilt();
  }

  private TNODE constructTree(String jsonStr, List<TNODE> leaves, int depth) {
    Gson gson = new Gson();
    TNODE parent = constructTNode(jsonStr);
    parent.setDepth(depth);
    JsonObject object = gson.fromJson(jsonStr, JsonObject.class);
    JsonElement element = object.get("children");
    if (element == null) {
      leaves.add(parent);
      return parent;
    }
    JsonArray array = element.getAsJsonArray();
    if (array.size() == 0) {
      leaves.add(parent);
      return parent;
    }
    List<TNODE> childrenList = new ArrayList<>();
    for (int i = 0; i < array.size(); ++i) {
      TNODE child = constructTree(array.get(i).toString(), leaves, depth + 1);
      child.setParent(parent);
      childrenList.add(child);
    }
    parent.setChildren(childrenList);
    onRelationBuilt(parent, childrenList);
    return parent;
  }

  public TNODE getRoot() {
    return root;
  }

  public List<TNODE> getLeaves() {
    return leaves;
  }

  public String getVersion() {
    return version;
  }

  public void onRelationBuilt(TNODE parent, List<TNODE> children) {

  }

  public void onTreeBuilt(){

  }

}
