package com.demo.accelator

import org.apache.spark.sql._

import com.demo.utils.CommonConstants
import com.demo.utils.CommonUtils
import com.demo.utils.InitSparkContext

object Start {
  def main(args: Array[String]) {

    val xmlDataDF = InitSparkContext.getSqlContext().read
      .format("com.databricks.spark.xml")
      .option("rowTag", CommonConstants.rowTag)
      .load("datasources/sample.xml")

    val finalDF = CommonUtils.selectElements(xmlDataDF, CommonConstants.rowTag)
    finalDF.show

  }
}