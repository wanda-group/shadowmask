package org.shadowmask.engine.spark

import org.apache.spark.sql.DataFrame

class RiskBasedAnonymizer(val df: DataFrame, val config: AnonymizationConfig) {

}

object RiskBasedAnonymizer {
  def apply(df: DataFrame, config: AnonymizationConfig): Unit = {
    new RiskBasedAnonymizer(df, config)
  }
}
