package cubeoperator

import org.apache.spark.rdd._

class CubeOperator(reducers: Int) {

  // MRDataCube operator.
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
      .map(aggregator.mapper) // map each input row to an RDD row
      // combine locally, shuffle and reduce
      .reduceByKey(aggregator.reducer, numPartitions = reducers)
      .flatMap(aggregator.partialGenerator) // generate partial upper cells

    // execute phase 2
    val phaseTwoResult = phaseOneResult
      // reducer is the same as in the first phase
      .reduceByKey(aggregator.reducer, numPartitions = reducers)

    // apply epilogue mapper and return result
    phaseTwoResult.mapValues(aggregator.epilogueMapper)
  }

  // Naive cube operator.
  def cube_naive(dataSet: Dataset,
                 groupingAttributes: List[String],
                 aggAttribute: String,
                 agg: String): RDD[(String, Double)] = {

    val rdd = dataSet.getRDD
    val schema = dataSet.getSchema

    val indexAtt = groupingAttributes.map { schema.indexOf(_) }
    val indexAgg = schema.indexOf(aggAttribute)

    // get the appropriate naive aggregator object providing the map/reduce functions
    val aggregator = NaiveCubeAggregator(agg, indexAtt, indexAgg)

    // import implicit ClassTags for Key and Value
    // required by the PairRDDFunctions constructor, called by reduceByKey
    import aggregator._

    rdd
      .flatMap(aggregator.mapper)
      .reduceByKey(aggregator.reducer, numPartitions = reducers)
      .mapValues(aggregator.epilogueMapper)
  }

}

object CubeOperator {
  def apply(reducers: Int) = new CubeOperator(reducers)
}
