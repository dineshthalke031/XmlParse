package com.demo.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object InitSparkContext {

  private val sparkContext = new SparkContext("local[*]", "Xml Parser")
  private val sqlContext = new SQLContext(sparkContext)

  val prop = System.getProperties
  prop.setProperty("hadoop.home.dir", "C:\\Dinesh\\winutils\\")
  System.setProperties(prop);

  def getSqlContext(): SQLContext = {
    this.sqlContext
  }

  def getSparkContext(): SparkContext = {
    this.sparkContext
  }
}