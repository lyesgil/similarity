package com.suggestion.similarity.cco


import org.apache.mahout.sparkbindings._
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.spark.SparkContext
import org.apache.mahout.sparkbindings.indexeddataset.IndexedDatasetSpark
import org.apache.mahout.math.indexeddataset.{BiDictionary, IndexedDataset}
import org.apache.mahout.math.cf.SimilarityAnalysis


class CcoAnalysis2 {
  def process(sc:SparkContext, dataFile:String): Unit = {
    implicit val mc = mahoutSparkContext("local","test")
    //implicit val sdc: org.apache.mahout.sparkbindings.SparkDistributedContext = sc2sdc(sc)

    // We need to turn our raw text files into RDD[(String, String)]
    val userTagsRDD = sc.textFile("src\\main\\resources\\data\\user_taggedartists.dat").map(line => line.split("\t")).map(a => (a(0), a(2))).filter(_._1 != "userID")
    val userTagsIDS = IndexedDatasetSpark.apply(userTagsRDD)(sc)

    val userArtistsRDD = sc.textFile("src\\main\\resources\\data\\user_artists.dat").map(line => line.split("\t")).map(a => (a(0), a(1))).filter(_._1 != "userID")
    val userArtistsIDS = IndexedDatasetSpark.apply(userArtistsRDD)(sc)

    val userFriendsRDD = sc.textFile("src\\main\\resources\\data\\user_friends.dat").map(line => line.split("\t")).map(a => (a(0), a(1))).filter(_._1 != "userID")
    val userFriendsIDS = IndexedDatasetSpark.apply(userFriendsRDD)(sc)

    val primaryIDS = userFriendsIDS
    val secondaryActionRDDs = List(userArtistsRDD, userTagsRDD)


    def adjustRowCardinality(rowCardinality: Integer, datasetA: IndexedDataset): IndexedDataset = {
      val returnedA = if (rowCardinality != datasetA.matrix.nrow) datasetA.newRowCardinality(rowCardinality)
      else datasetA // this guarantees matching cardinality

      returnedA
    }

    var rowCardinality = primaryIDS.rowIDs.size

    val secondaryActionIDS: Array[IndexedDataset] = new Array[IndexedDataset](secondaryActionRDDs.length)
    for (i <- secondaryActionRDDs.indices) {

      val bcPrimaryRowIDs = sc.broadcast(primaryIDS.rowIDs)
      bcPrimaryRowIDs.value

      val tempRDD = secondaryActionRDDs(i).filter(a => bcPrimaryRowIDs.value.contains(a._1))

      var tempIDS = IndexedDatasetSpark.apply(tempRDD, existingRowIDs = Some(primaryIDS.rowIDs))(sc)
      secondaryActionIDS(i) = adjustRowCardinality(rowCardinality,tempIDS)
    }



    val artistReccosLlrDrmListByArtist = SimilarityAnalysis.cooccurrencesIDSs(
      Array(primaryIDS, secondaryActionIDS(0), secondaryActionIDS(1)),
      maxInterestingItemsPerThing = 20,
      maxNumInteractions = 500,
      randomSeed = 1234)

  }
}
