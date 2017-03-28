package org.shadowmask.engine.spark.expressions

import org.apache.spark.unsafe.types.UTF8String
import org.shadowmask.core.mask.rules.generalizer.impl.IPGeneralizer

object MaskExpressionsUtil {

  val emptyUTF8Str: UTF8String = UTF8String.fromString("")

  def maskIp(cell: Any, level: Int): UTF8String = {
    if (cell == null) {
      return emptyUTF8Str
    }

    val generalizer = new IPGeneralizer()
    cell match {
      case str: String =>
        UTF8String.fromString(generalizer.generalize(str, level))
      case utf8Str: UTF8String =>
        UTF8String.fromString(generalizer.generalize(utf8Str.toString, level))
      case _ =>
        emptyUTF8Str
    }

  }
}
