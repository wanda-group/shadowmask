package com.shadowmask.core.domain;

import org.junit.Assert;
import org.junit.Test;
import org.shadowmask.core.domain.StringDomainTree;

public class TestStringDomainTree {

  @Test
  public void testTree(){
    StringDomainTree tree = new StringDomainTree("{\n" + "  \"comparable\": \"false\",\n"
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

    Assert.assertEquals(tree.getLeaves().size(),5);
  }

}
