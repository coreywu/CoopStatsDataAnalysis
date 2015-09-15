package com.corey.algorithms

import scala.collection.immutable.HashSet
import scala.io.Source
import scala.collection.mutable.Buffer
import scala.util.Random

object RandomWalk {
  
  def main(args: Array[String]) = {
    val iterations = 10000
    val filename = "src/main/resources/waterloo_raw.txt"
    
    val coops: Buffer[Buffer[String]] = Buffer()

    for (line <- Source.fromFile(filename).getLines()) {
      val companies: Buffer[String] = Buffer()
      val companiesString = line.substring(1, line.length - 1)
      
      var firstIndex = 0
      while (firstIndex < companiesString.length - 1) {
        var firstQuote = companiesString.charAt(firstIndex)
        var secondIndex = companiesString.indexOf(firstQuote, firstIndex + 1)
        companies += companiesString.substring(firstIndex + 1, secondIndex)
        
        firstIndex = secondIndex + 3
      }
      coops += companies
    }
    
    var firstCompanyList: List[String] = List()
    var nextCoopList: List[(String, String)] = List()
    for (companyNames <- coops) {
      val start = if (companyNames.length > 6) (companyNames.length - 6) else 0
      firstCompanyList +:= companyNames(start)
      for (i <- start until companyNames.length - 1) {
        val firstCompanyName = companyNames(i)
        val secondCompanyName = companyNames(i + 1) 
        val term = i - start
        nextCoopList +:= (firstCompanyName, secondCompanyName)
      }
    }
    
    val nextCoopMap: Map[String, List[String]] = nextCoopList.groupBy(_._1).map {
      case (companyName, tupleList) => (companyName, tupleList.map(_._2))
    }

    val companiesForEachTerm: Array[List[String]] = Array.fill(6)(List())
    
    for (i <- 0 until iterations) {
    	val randomFirstCompany = firstCompanyList(Random.nextInt(firstCompanyList.length))
    	companiesForEachTerm(0) +:= randomFirstCompany 
    	var company = randomFirstCompany
    
    	for (term <- 1 until 6) {
    		if (nextCoopMap.contains(company)) {
    			company = nextCoopMap(company)(Random.nextInt(nextCoopMap(company).length))
    					companiesForEachTerm(term) +:= company
    		}
    	}
    }
    
    println(companiesForEachTerm.mkString("\n"))
    println()
    
    val companyOccurencesForEachTerm: Array[Map[String, Int]] = companiesForEachTerm.map {
      _.groupBy(identity).mapValues(_.size)
    }
    
    val sortedCompanyOccurencesForEachTerm: Array[List[String]] = companyOccurencesForEachTerm.map {
      map: Map[String, Int] => map.toList.sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int).map(_._1).reverse
    }
    
    println(sortedCompanyOccurencesForEachTerm.mkString("\n"))
  }
}