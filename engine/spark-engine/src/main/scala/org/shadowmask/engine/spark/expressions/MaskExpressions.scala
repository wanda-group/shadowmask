package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.types.{StringType, DateType, AbstractDataType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.shadowmask.engine.spark.expressions.MaskExpressionsUtil

object MaskExpressions {

  private def withExpr(expr: Expression): Column = Column(expr)

  def ipMask(e: Column, level: Int): Column = withExpr {
    IPMask(e.expr, level)
  }

}

case class IPMask(child: Expression, level: Int) extends UnaryExpression with String2StringExpression {

  override def dataType: DataType = StringType

  override protected def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {

    defineCodeGen(ctx, ev, c => s"org.shadowmask.engine.spark.expressions.MaskExpressionsUtil.maskIp($c, $level)")
  }

  override def convert(v: UTF8String): UTF8String = {
    MaskExpressionsUtil.maskIp(v, level)
  }
}
