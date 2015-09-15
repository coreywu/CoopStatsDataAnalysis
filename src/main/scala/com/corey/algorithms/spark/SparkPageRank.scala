package com.corey.algorithms

import scala.collection.immutable.HashMap
import scala.collection.mutable.Buffer
import scala.io.Source
import scala.collection.immutable.HashSet
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

import com.corey.algorithms.PageRank._

object SparkPageRank {
  
  def main(args: Array[String]) {
    val filename = "src/main/resources/waterloo_raw.txt"
    val coopCompanyGraph: GraphBuilder.CoopCompanyGraph = GraphBuilder.buildCoopCompanyGraph(filename)
    
    // A map of companies to forwardly-linked companies and the total weight
    // of forward links to each company, represented as a tuple in the form of
    // (company, weight)
    var weightedLinksSeq: Seq[(String, (Set[(String, Int)], Int, Double))] = Seq()
    
    for (company <- coopCompanyGraph.getCompanyMap().values) {
      var forwardLinksSet: Set[(String, Int)] = HashSet()

      var totalWeight = 0
      
      for (forwardCompanyLinks: (String, Buffer[GraphBuilder.Link]) <- company.forwardLinks) {
        var totalCompanyWeight = 0
        for (forwardLink: GraphBuilder.Link <- forwardCompanyLinks._2) {
          totalCompanyWeight += forwardLink.weight
        }
        forwardLinksSet += ((forwardCompanyLinks._1, totalCompanyWeight))
        totalWeight += totalCompanyWeight
      }
      weightedLinksSeq +:= (company.name, (forwardLinksSet, totalWeight, company.count.toDouble))
    }
    
    val conf = new SparkConf().setAppName("Page Rank Algorithm").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    
    val weightedLinksRDD: RDD[(String, (Set[(String, Int)], Int, Double))] = sc.parallelize(weightedLinksSeq)
        .partitionBy(new HashPartitioner(100))
        .persist()
    
    var ranks: RDD[(String, Double)] = weightedLinksRDD.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      val ranksMap = ranks.collectAsMap()

      weightedLinksRDD.map {
        case (companyName, (weightedLinks, totalWeight, rank)) => (companyName, (weightedLinks, totalWeight, ranksMap(companyName)))
      }

      val contributions: RDD[(String, Double)] = weightedLinksRDD.flatMap {
        case (companyName, (weightedLinks, totalWeight, rank)) => weightedLinks.map(weightedLink => (weightedLink._1, rank * weightedLink._2 / totalWeight)) 
      }
    
      val nonZeroRanksMap = contributions.groupBy(_._1).map {
        case (key, value) => (key, value.map(_._2).reduce((x, y) => x + y))
      }.collectAsMap()
      
      ranks = ranks.map{case (companyName, rank) => (companyName, nonZeroRanksMap.getOrElse(companyName, 0.0))}
    }
    
    ranks = ranks.sortBy(_._2, false)

    val companyScores = ranks.map {
      case (companyName, rank) => (companyName, rank / coopCompanyGraph.getCompany(companyName).count)
    }.sortBy(_._2, false)
    
    val sqrtCompanyScores = ranks.map {
      case (companyName, rank) => (companyName, rank / math.sqrt(coopCompanyGraph.getCompany(companyName).count))
    }.sortBy(_._2, false)

    val logCompanyScores = ranks.map {
      case (companyName, rank) => (companyName, rank / math.log(coopCompanyGraph.getCompany(companyName).count + math.E))
    }.sortBy(_._2, false)
    
    ranks.coalesce(1).saveAsTextFile("spark_results/page_ranks")
    companyScores.coalesce(1).saveAsTextFile("spark_results/page_ranks_per_student")
    sqrtCompanyScores.coalesce(1).saveAsTextFile("spark_results/page_ranks_per_sqrt_student")
    logCompanyScores.coalesce(1).saveAsTextFile("spark_results/page_ranks_per_log_student")
  }
  
}