package com.shadowmask.core.domain;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.shadowmask.core.domain.tree.DomainTree;

public class DomainTestTree extends DomainTree<TestTreeNode> {
  @Override protected TestTreeNode constructTNode(String jsonStr) {
    Gson gson = new Gson();
    TestTreeNode node = new TestTreeNode();
    node.setName(
        gson.fromJson(jsonStr, JsonObject.class).get("text").getAsString());
    return node;
  }

  public DomainTestTree() {
    this.constructFromJson("{\n" + "  \"comparable\": \"false\",\n"
        + "  \"type\": \"string\",\n" + "  \"version\": \"1.0\",\n"
        + "  \"root\": {\n" + "    \"text\": \"*\",\n" + "    \"children\": [\n"
        + "      {\n" + "        \"text\": \"上海\",\n"
        + "        \"children\": [\n" + "          {\n"
        + "            \"text\": \"上海,闵行\",\n" + "            \"children\": [\n"
        + "              {\n" + "                \"text\": \"上海,闵行,浦江镇\"\n"
        + "              },\n" + "              {\n"
        + "                \"text\": \"上海,闵行,三林\"\n" + "              }\n"
        + "            ]\n" + "          },\n" + "          {\n"
        + "            \"text\": \"上海,浦东\",\n" + "            \"children\": [\n"
        + "              {\n" + "                \"text\": \"上海,浦东,塘桥\"\n"
        + "              }\n" + "            ]\n" + "          }\n"
        + "        ]\n" + "      },\n" + "      {\n"
        + "        \"text\": \"北京\",\n" + "        \"children\": [\n"
        + "          {\n" + "            \"text\": \"北京,东城\",\n"
        + "            \"children\": [\n" + "              {\n"
        + "                \"text\": \"北京,东城,西直门\"\n" + "              },\n"
        + "              {\n" + "                \"text\": \"北京,东城,宣武门\"\n"
        + "              }\n" + "            ]\n" + "          }\n"
        + "        ]\n" + "      }\n" + "    ]\n" + "  }\n" + "}");
  }


}
