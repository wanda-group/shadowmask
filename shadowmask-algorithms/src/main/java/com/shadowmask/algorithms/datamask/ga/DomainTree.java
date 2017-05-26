package com.shadowmask.algorithms.datamask.ga;

import com.google.gson.Gson;
import java.util.List;

public class DomainTree<TNODE extends DomainTreeNode> {
  private int height;
  private TNODE root;
  private List<TNODE> leaves;

  /**
   * construct domain tree from json string
   *
   * @param jsonStr: json string:
   */
  /*
    {
      comparable: true/false,
      type: integer/float/string
      version:1.0,
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

  }

}
