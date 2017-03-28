package org.shadowmask.engine.spark.functions

import org.shadowmask.core.mask.rules.generalizer.impl.IPGeneralizer
import org.apache.spark.sql.functions.udf

object maskFunctions {

  val maskIP = udf(ipMask(_: String, _: Int))

  def ipMask(ip: String, level: Int): String = {
    val generalizer = new IPGeneralizer()
    generalizer.generalize(ip, level)
  }

}