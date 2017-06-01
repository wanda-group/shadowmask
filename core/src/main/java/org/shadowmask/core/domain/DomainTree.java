package org.shadowmask.core.domain;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;

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

  /**
   * construct domain tree from json string
   *
   * @param jsonStr: json string:
   */
  /*
    {
      comparable: true/false,
      type: integer/float/string
      version:"1.0",
      root:{
        text: "some name",
        lbound: 0, //when comparable is true.
        hbound: 30, //when comparable is true.
        children:[
          {
            text:"some text",
            lbound: 0,
            hbound: 10
            children:[
              {
                ...
              }
            ]
          },{

          },
          ...
          ,{
          }
        ]
      }
    }
     */
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
}
