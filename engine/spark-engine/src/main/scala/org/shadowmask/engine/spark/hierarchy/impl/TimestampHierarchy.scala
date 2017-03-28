package org.shadowmask.engine.spark.hierarchy.impl

import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.shadowmask.engine.spark.hierarchy.Hierarchy
import org.shadowmask.engine.spark.hierarchy.mask.TimestampRule

class TimestampHierarchy(alignLeft: Boolean,
                         maskLeft: Boolean,
                         maskChar: Char = '*') extends Hierarchy[String, String]{

  override def rootHierarchyLevel: Int = -1

  override def getUDF(hierarchy: Int): UserDefinedFunction = udf(getTimestampRule(hierarchy))

  def getTimestampRule(hierarchy: Int): (String) => String = {
    new TimestampRule(hierarchy, maskChar).mask
  }
}
