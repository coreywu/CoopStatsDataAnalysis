package com.corey.algorithms

import scala.collection.mutable.Buffer
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.corey.algorithms.PageRank._
import com.corey.algorithms.spark.GraphXGraphBuilder

object GraphXTriangleCounting {

  def main(args: Array[String]) {
    val filename = "src/main/resources/waterloo_raw.txt"
    val graph = GraphXGraphBuilder.buildGraphXCoopCompanyGraph(filename)   

//    val triangleCounts = graph.triangleCount().vertices
    val triangleCounts = graph.triangleCount().vertices
    
    val triangleCountsByCompany = graph.vertices.join(triangleCounts).map {
      case (id, (companyName, triangleCount)) => (companyName, triangleCount)
    }
    
    println(triangleCountsByCompany.collect().mkString("\n"))
//    triangleCountsByCompany.coalesce(1).saveAsTextFile("spark_results/triangle_counts_graphx")

//    val connectedComponentsMap = triangleCountsByCompany.toArray.toMap
//    val verticesMap = graph.vertices.toArray.toMap
//    groupedConnectedComponents.coalesce(1).saveAsTextFile("spark_results/grouped_connected_components_graphx")
  }
  
}