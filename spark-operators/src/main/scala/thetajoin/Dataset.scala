package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class Dataset(val rdd: RDD[Row], val schema: List[String]) {
  def getRDD: RDD[Row] = rdd

  def getSchema: List[String] = schema
}

object Dataset {
  def apply(rdd: RDD[Row], schema: List[String]) = new Dataset(rdd, schema)
}
