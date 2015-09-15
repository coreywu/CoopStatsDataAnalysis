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
import scala.util.Random
import org.apache.spark.AccumulatorParam

object SparkRandomWalk {
  
  type CompanyOccurrences = Map[String, Int]
  
  class CompanyOccurrencesPerTermAP extends AccumulatorParam[Array[CompanyOccurrences]] {
    def zero(array: Array[CompanyOccurrences]) = new Array(6)
    
    def addInPlace(array1: Array[CompanyOccurrences], array2: Array[CompanyOccurrences]) = {
      for (i <- 0 until 6) {
        for ((company, occurrences2) <- array2(i)) {
          if (array1(i).contains(company)) {
            val occurrences1 = array1(i)(company)
            array1(i) += ((company, occurrences1 + occurrences2))
          }
        }
      }
      array1
    }
  }
  
  def main(args: Array[String]) {
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
    
    val conf = new SparkConf().setAppName("Random Walk Algorithm").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    
    val coopsRDD: RDD[Buffer[String]] = sc.parallelize(coops).persist()
    
//    var firstCompanyRDD: RDD[String] = 
//    var nextCoopRDD: RDD[(String, String)] = List()

    val firstCompanyRDD: RDD[String] = for {
      companyNames <- coopsRDD
      start = if (companyNames.length > 6) (companyNames.length - 6) else 0
    } yield companyNames(start)

    val nextCoopRDD: RDD[(String, String)] = for {companyNames <- coopsRDD
      val start = if (companyNames.length > 6) (companyNames.length - 6) else 0
      i <- start until companyNames.length - 1
      firstCompanyName = companyNames(i)
      secondCompanyName = companyNames(i + 1) 
      term = i - start
    } yield (firstCompanyName, secondCompanyName)
    
//    val firstCompanyRDD: RDD[String] = sc.parallelize(firstCompanyList).persist()
//    val nextCoopRDD: RDD[(String, String)] = sc.parallelize(nextCoopList).persist()
    
    val nextCoopMapRDD: RDD[(String, Iterable[String])] = nextCoopRDD.groupBy(_._1).map {
      case (companyName, tupleList) => (companyName, tupleList.map(_._2))
    }

    val companiesForEachTermRDD: RDD[List[String]] = sc.parallelize(Array.fill(6)(List()))
    
    val iterationsOverCountFraction = iterations / firstCompanyRDD.count
    val randomFirstCompaniesRDD = firstCompanyRDD.sample(true, iterationsOverCountFraction)
    
//    companiesForEachTermRDD.zipWithIndex().map {
//      case (companiesList, termIndex) => if (termIndex > 0) {
//    	  for {
//    		  company <- companiesForEachTermRDD.
//
//    		  term <- 1 until 6
//    		  if (nextCoopMapRDD.lookup(company).size > 0) {
//    			  company = nextCoopMapRDD.lookup(company)(0).drop(Random.nextInt(nextCoopMapRDD.lookup(company).length)).iterator.next()
//    					  companiesForEachTerm(term) +:= company
//    		  }
//    	  } yield company
//      }
//    }
    
//    println(companiesForEachTerm.mkString("\n"))
//    println()
    
//    val companyOccurencesForEachTerm: Array[Map[String, Int]] = companiesForEachTerm.map {
//      _.groupBy(identity).mapValues(_.size)
//    }
//    
//    val sortedCompanyOccurencesForEachTerm: Array[List[String]] = companyOccurencesForEachTerm.map {
//      map: Map[String, Int] => map.toList.sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int).map(_._1).reverse
//    }
//    
//    println(sortedCompanyOccurencesForEachTerm.mkString("\n"))
  }
  
}