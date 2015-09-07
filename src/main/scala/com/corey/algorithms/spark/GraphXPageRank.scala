package com.corey.algorithms

import scala.collection.immutable.HashMap
import scala.collection.mutable.Buffer
import scala.io.Source
import scala.collection.immutable.HashSet
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import com.corey.algorithms.spark.GraphXGraphBuilder

object GraphXPageRank {

  def main(args: Array[String]) {
    val filename = "src/main/resources/waterloo_raw.txt"
    val graph = GraphXGraphBuilder.buildGraphXCoopCompanyGraph(filename)

    val ranks = graph.pageRank(0.0001).vertices
    val ranksByCompany = graph.vertices.join(ranks).map {
      case (id, (companyName, rank)) => (companyName, rank)
    }
    
    ranksByCompany.sortBy(_._2, false).coalesce(1).saveAsTextFile("spark_results/page_rank_graphx")
  }
  
}