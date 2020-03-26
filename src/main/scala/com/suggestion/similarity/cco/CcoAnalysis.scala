package com.suggestion.similarity.cco


import org.apache.mahout.math.cf.SimilarityAnalysis
import org.apache.mahout.sparkbindings._
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.SparkContext

class CcoAnalysis {
  def process(ctx:SparkContext, dataFile:String): Unit = {
    implicit val sdc: org.apache.mahout.sparkbindings.SparkDistributedContext = sc2sdc(ctx)

    val data = ctx.textFile(dataFile)

    val purchasesRDD =  data
      .map(line => line.split(","))
      .map(a => (a(0), a(2)))
      .filter(_._1  == "purchase")

    val viewsRDD =  data
      .map(line => line.split(","))
      .map(a => (a(0), a(2)))
      .filter(_._1  == "view")

    val purchasesIDS = IndexedDatasetSpark.apply(purchasesRDD)(ctx)
    val viewIDS = IndexedDatasetSpark.apply(viewsRDD)(ctx)

    //val purchasesIDS = IndexedDatasetSpark.apply(inputRDD.filter(_._2 == "purchase").map(o => (o._1, o._3)))(ctx)
    //val browseIDS = IndexedDatasetSpark.apply(inputRDD.filter(_._2 == "category-browse").map(o => (o._1, o._3)))(ctx)

    val llrDrmList = SimilarityAnalysis.cooccurrencesIDSs(Array(purchasesIDS, viewIDS),
      randomSeed = 1234,
      maxInterestingItemsPerThing = 4,
      maxNumInteractions = 4)

    val llrAtA = llrDrmList(0).matrix.collect
  }
}
