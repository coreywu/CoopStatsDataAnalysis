package com.corey.algorithms

import scala.collection.immutable.HashMap
import scala.collection.immutable.Map
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
import org.apache.spark.Accumulator
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter

object SparkRandomWalk {
  
  type CompanyOccurrences = Map[String, Int]
  
  class CompanyOccurrencesAP extends AccumulatorParam[CompanyOccurrences] {
    def zero(companyOccurrences: CompanyOccurrences) = HashMap()
    
    def addInPlace(companyOccurrences1: CompanyOccurrences, companyOccurrences2: CompanyOccurrences) = {
      var newCompanyOccurrences = HashMap() ++ companyOccurrences1
    	for ((company, occurrences2) <- companyOccurrences2) {
    		if (companyOccurrences1.contains(company)) {
    			val occurrences1 = companyOccurrences1(company)
    		  newCompanyOccurrences += ((company, occurrences1 + occurrences2))
    		} else {
    		  newCompanyOccurrences += ((company, occurrences2))
        }
    	}
      newCompanyOccurrences
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
    
    val firstCompanyRDD: RDD[String] = for {
      companyNames <- coopsRDD
      start = if (companyNames.length > 6) (companyNames.length - 6) else 0
    } yield companyNames(start)

    val nextCoopRDD: RDD[(String, String)] = for {
      companyNames <- coopsRDD
      start = if (companyNames.length > 6) (companyNames.length - 6) else 0
      i <- start until companyNames.length - 1
      firstCompanyName = companyNames(i)
      secondCompanyName = companyNames(i + 1) 
      term = i - start
    } yield (firstCompanyName, secondCompanyName)
    
    val nextCoopMap: Map[String, Iterable[String]] = nextCoopRDD.groupBy(_._1).map {
      case (companyName, tupleList) => (companyName, tupleList.map(_._2))
    }.collect.toMap

    val iterationsOverCountFraction = iterations / firstCompanyRDD.count
    val randomFirstCompaniesRDD = firstCompanyRDD.sample(true, iterationsOverCountFraction)

    val occurrencesPerTerm: Array[Accumulator[CompanyOccurrences]] = Array.fill(6)(sc.accumulator[CompanyOccurrences](Map[String, Int]())(new CompanyOccurrencesAP))
    
    for (randomFirstCompany <- randomFirstCompaniesRDD) {
      var company = randomFirstCompany
      occurrencesPerTerm(0) += Map(company -> 1)
      
      for (term <- 1 until 6) {
        if (nextCoopMap.contains(company)) {
          company = nextCoopMap(company).drop(Random.nextInt(nextCoopMap(company).size)).iterator.next()
          occurrencesPerTerm(term) += Map(company -> 1)
        }
      }
    }
    
    val sortedCompanyOccurrencesPerTerm: Array[List[String]] = occurrencesPerTerm.map {
      accumulator: Accumulator[Map[String, Int]] => accumulator.value.toList.sortBy((tuple: (String, Int)) => tuple._2)(Ordering.Int).map(_._1).reverse
    }

    val stringBuilder = new StringBuilder()
    
    for (i <- 1 to 6) {
      stringBuilder.append("Term " + i + ": ")
      stringBuilder.append(sortedCompanyOccurrencesPerTerm(i - 1).take(10).mkString(", "))
      stringBuilder.append("\n")
    }
    
    println(stringBuilder.toString())

    val bufferedWriter = new BufferedWriter(new FileWriter(new File("spark_results/random_walk_spark")))
    bufferedWriter.write(stringBuilder.toString())
    bufferedWriter.close()
  }
  
}