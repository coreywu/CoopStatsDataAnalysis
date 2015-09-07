package com.corey.algorithms

import scala.collection.immutable.HashMap
import scala.collection.mutable.Buffer
import scala.io.Source
import scala.collection.immutable.HashSet

object PageRank {

  def main(args: Array[String]) {
    val filename = "src/main/resources/waterloo_raw.txt"
    val coopCompanyGraph: GraphBuilder.CoopCompanyGraph = GraphBuilder.buildCoopCompanyGraph(filename) 

    val dampingFactor = 0.85

    // A map of companies to forwardly-linked companies and the total weight
    // of forward links to each company, represented as a tuple in the form of
    // (company, weight)
    var weightedLinksMap: scala.collection.mutable.Map[String, (Set[(String, Int)], Int, Double)] = scala.collection.mutable.HashMap()
    
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
      weightedLinksMap += company.name -> (forwardLinksSet, totalWeight, 1)
    }
    
    var ranks: scala.collection.Map[String, Double] = weightedLinksMap.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      weightedLinksMap.map {
        case (companyName, (weightedLinks, totalWeight, rank)) => (companyName, (weightedLinks, totalWeight, ranks(companyName)))
      }

      val contributions = weightedLinksMap.toList.flatMap {
        case (companyName, (weightedLinks, totalWeight, rank)) => weightedLinks.map(weightedLink => (weightedLink._1, rank * weightedLink._2 / totalWeight)) 
      }
    
      val nonZeroRanks = contributions.groupBy(_._1).map {
        case (key, value) => (key, value.map(_._2).reduce((x, y) => x + y))
      }.toMap
      
      ranks = ranks.map {
        case (companyName, rank) => (companyName, 1 - dampingFactor + dampingFactor * nonZeroRanks.getOrElse(companyName, 0.0))
      }
    }
    
    val companyScores = ranks.map {
      case (companyName, rank) => (companyName, rank / coopCompanyGraph.getCompany(companyName).count)
    }
    
    val sqrtCompanyScores = ranks.map {
      case (companyName, rank) => (companyName, rank / math.sqrt(coopCompanyGraph.getCompany(companyName).count))
    }

    println(ranks.toList.sortBy(_._2).reverse.take(10))
    println(sqrtCompanyScores.toList.sortBy(_._2).reverse.take(10))
    println(companyScores.toList.sortBy(_._2).reverse.take(10))
  }
  
}