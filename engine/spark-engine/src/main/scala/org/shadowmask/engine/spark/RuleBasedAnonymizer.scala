package org.shadowmask.engine.spark

import org.apache.spark.sql.{Column, DataFrame}
import org.shadowmask.core.AnonymityFieldType

class RuleBasedAnonymizer(var df: DataFrame, val config: AnonymizationConfig) {

  def anonymize(): RuleBasedAnonymizer = {
    val attributes: Array[FieldAttribute] = config.fieldAttributes
    val identifiers = attributes.filter(_.anonymityType() == AnonymityFieldType.IDENTIFIER)
    val qusiIdentifiers = attributes.filter(_.anonymityType() == AnonymityFieldType.QUSI_IDENTIFIER)
    val sensitiveFields = attributes.filter(_.anonymityType() == AnonymityFieldType.SENSITIVE)
    val nonSensitiveFields = attributes.filter(_.anonymityType() == AnonymityFieldType.NON_SENSITIVE)

    val privateFields: Array[FieldAttribute] = identifiers ++ qusiIdentifiers
    val hierarchyFields: Array[RuleHierarchyFieldAttribute] =
      privateFields
        .filter(_.isInstanceOf[RuleHierarchyFieldAttribute])
        .map(_.asInstanceOf[RuleHierarchyFieldAttribute])
    val aggregateFields: Array[MicroAggregateFieldAttribute] =
      privateFields
        .filter(_.isInstanceOf[MicroAggregateFieldAttribute])
        .map(_.asInstanceOf[MicroAggregateFieldAttribute])

    val maskedFieldnames = hierarchyFields.map(_.name()).map(_ + "_masked")
    val maskedFields: Array[Column] = hierarchyFields.zip(maskedFieldnames).map {
      case (fieldAttribute, maskedName) =>
        val fieldName = fieldAttribute.name()
        fieldAttribute.rule(new Column(fieldName)).as(maskedName)
    }

    val selectedFiedls = maskedFields ++ sensitiveFields.map(_.name).map(new Column(_)) ++
      nonSensitiveFields.map(_.name).map(new Column(_)) ++
      aggregateFields.map(_.name).map(new Column(_))

    val maskedDF: DataFrame = df.select(selectedFiedls: _*)

    println("=======print rule masked table")
    maskedDF.show(100)

    val aggregators: Array[Column] = aggregateFields.map {
      case fieldAttribute =>
        fieldAttribute.aggregator(new Column(fieldAttribute.name))
    }

    val groupByFields = maskedFieldnames.map(new Column(_))
    val groupedDF = if (aggregators.length == 1) {
      maskedDF.groupBy(groupByFields: _*).agg(aggregators(0))
    } else {
      maskedDF.groupBy(groupByFields: _*).agg(aggregators(0), aggregators.drop(0): _*)
    }

    println("========print grouped agg table")
    groupedDF.show(100)


    val nonAggregatorFields = maskedFieldnames.map(new Column(_)) ++
      sensitiveFields.map(_.name).map(new Column(_)) ++
      nonSensitiveFields.map(_.name).map(new Column(_))
    df = maskedDF.select(nonAggregatorFields: _*).join(groupedDF, maskedFieldnames.toSeq)

    println("========print joined table")
    df.show(100)
    this
  }
}

object RuleBasedAnonymizer {
  def apply(df: DataFrame, config: AnonymizationConfig): RuleBasedAnonymizer = {
    new RuleBasedAnonymizer(df, config)
  }
}
