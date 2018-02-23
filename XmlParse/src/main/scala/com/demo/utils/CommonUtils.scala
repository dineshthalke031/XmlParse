package com.demo.utils

import org.apache.spark.sql.DataFrame

object CommonUtils {

  def selectElements(mainDF: DataFrame, rowTag: String): DataFrame = {

    val tags = InitSparkContext.getSqlContext().read.format("com.databricks.spark.csv")
      .load("datasources/elements.csv")

    val selectedData = tags.map(row => {
      row.getAs[String](0).split("#").filter(!_.equals(rowTag)).reverse.mkString(".")
    }).collect()

    mainDF.select(selectedData.head, selectedData.tail: _*)
  }
}