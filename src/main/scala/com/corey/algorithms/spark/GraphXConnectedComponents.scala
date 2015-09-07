package com.corey.algorithms

import scala.collection.mutable.Buffer
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.corey.algorithms.PageRank._
import com.corey.algorithms.spark.GraphXGraphBuilder

object GraphXConnectedComponents {

  def main(args: Array[String]) {
    val filename = "src/main/resources/waterloo_raw.txt"
    val graph = GraphXGraphBuilder.buildGraphXCoopCompanyGraph(filename)   

    val connectedComponents = graph.connectedComponents().vertices
    
    val connectedComponentsByCompany = graph.vertices.join(connectedComponents).map {
      case (id, (companyName, connectedComponents)) => (companyName, connectedComponents)
    }
    
    connectedComponentsByCompany.coalesce(1).saveAsTextFile("spark_results/connected_components_graphx")

    val connectedComponentsMap = connectedComponentsByCompany.collect.toMap
    val verticesMap = graph.vertices.collect.toMap
    
    val groupedConnectedComponents = connectedComponentsByCompany.map(_.swap).groupByKey().map {
      case (vertexId, companyList) => (verticesMap(vertexId), companyList)
    }

    groupedConnectedComponents.coalesce(1).saveAsTextFile("spark_results/grouped_connected_components_graphx")
  }
  
}