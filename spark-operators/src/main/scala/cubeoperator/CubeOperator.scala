package cubeoperator

import org.apache.spark.rdd._

class CubeOperator(reducers: Int) {

  /**
    * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
    * the attribute on which the aggregation is performed
    * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
    * and returns an RDD with the result in the form of < key: string, value: double > pairs.
    * The key is used to uniquely identify a group that corresponds to a certain combination of
    * attribute values.
    * You are free to do that following your own naming convention.
    * The value is the aggregation result.
    * You are not allowed to change the definition of this function or the names of the aggregate
    * functions.
    */

  /**
    * MRDataCube operator.
    */
  def cube(dataSet: Dataset,
           groupingAttributes: List[String],
           aggAttribute: String,
           agg: String): RDD[(String, Double)] = {

    val rdd = dataSet.getRDD
    val schema = dataSet.getSchema

    val indexAtt = groupingAttributes.map { schema.indexOf(_) }
    val indexAgg = schema.indexOf(aggAttribute)

    // get the appropriate aggregator object providing the map/reduce functions
    val aggregator = CubeAggregator(agg, indexAtt, indexAgg)

    // import implicit ClassTags for Key and Value
    // required by the PairRDDFunctions constructor, called by reduceByKey
    import aggregator._

    // execute phase 1
    val phaseOneResult = rdd
        .map(aggregator.mapper)                // map each input row to an RDD row
        .reduceByKey(aggregator.reducer)       // combine locally, shuffle and reduce
        .flatMap(aggregator.partialGenerator)  // generate partial upper cells

    // execute phase 2
    val phaseTwoResult = phaseOneResult
        .map(identity)                    // map just returns the <key, value> pair
        .reduceByKey(aggregator.reducer)  // reducer is the same as in the first phase

    // apply epilogue mapper and return result
    phaseTwoResult.map(aggregator.epilogueMapper)
  }

  /**
    * Naive cube operator.
    */
  def cube_naive(dataset: Dataset,
                 groupingAttributes: List[String],
                 aggAttribute: String,
                 agg: String): RDD[(String, Double)] = {

    // TODO naive algorithm for cube computation
    ???
  }

}

object CubeOperator {
  def apply(reducers: Int) = new CubeOperator(reducers)
}
