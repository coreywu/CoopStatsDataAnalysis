package com.corey.algorithms

import scala.collection.immutable.HashMap
import scala.collection.mutable.Buffer
import scala.io.Source
import scala.collection.immutable.HashSet
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object SparkPageRank {

  class Company(val name: String) extends Serializable {
    var count = 0
    var termCounts: Array[Int] = Array(0, 0, 0, 0, 0, 0)
    val forwardLinks: scala.collection.mutable.HashMap[String, Buffer[Link]] = scala.collection.mutable.HashMap()
    
    def addTerm(term: Int) = {
      count += 1
      termCounts(term) += 1
    }

    def addLink(link: Link) = {
      if (!forwardLinks.contains(link.secondCompanyName)) {
        forwardLinks.put(link.secondCompanyName, Buffer(link))
      } else {
    	  forwardLinks.update(link.secondCompanyName, forwardLinks(link.secondCompanyName) :+ link)
      }
    }
    
    override def toString(): String = {
      "Company: [name=" + name + ", count=" + count + ", termCounts=" + termCounts.mkString(" ") +
          ",forwardLinks=" + forwardLinks + "]"
    }
    
    override def hashCode(): Int = {
      val prime = 17 
      var result = 1
      result = prime * result + count
      result = prime * result + termCounts.hashCode() 
      result = prime * result + forwardLinks.hashCode()
      result
    }
  }
  
  class Link(val firstCompanyName: String, val firstTerm: Int, val secondCompanyName: String, val secondTerm: Int) 
      extends Serializable {
    val weight = secondTerm - firstTerm
    
    override def toString(): String = {
      "Link: [firstCompanyName=" + firstCompanyName + ", firstTerm=" + firstTerm  +
          ", secondCompanyName=" + secondCompanyName + ", secondTerm: " + secondTerm + ", weight=" + weight + "]"
    }
  }
  
  class CoopCompanyGraph() extends Serializable {
    private var companies: HashMap[String, Company] = HashMap()

    def contains(companyName: String): Boolean = companies.contains(companyName)
    
    def addTerm(companyName: String, term: Int): Unit = {
      if (!companies.contains(companyName)) {
        companies += companyName -> new Company(companyName)
      }
      companies(companyName).addTerm(term)
    }

    def addLink(firstCompanyName: String, firstTerm: Int, secondCompanyName: String, secondTerm: Int): Unit = {
      if (!companies.contains(firstCompanyName)) {
        companies += firstCompanyName -> new Company(firstCompanyName)
      }
      if (!companies.contains(secondCompanyName)) {
        companies += secondCompanyName -> new Company(secondCompanyName)
      }
      companies(firstCompanyName).addLink(new Link(firstCompanyName, firstTerm, secondCompanyName, secondTerm))
    }
    
    def take(n: Int): HashMap[String, Company] = {
      companies.take(n)
    }
    
    def getCompanyMap(): HashMap[String, Company] = companies

    def getCompany(companyName: String): Company = companies(companyName)

    override def toString(): String = {
      "CoopCompanyGraph: [companies=" + companies + "]"
    }

    override def hashCode(): Int = {
      val prime = 17 
      var result = 1
      result = prime * result + companies.hashCode()
      result
    }
  }
  
  def main(args: Array[String]) {
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
    
    // Create company graph 
    val coopCompanyGraph = new CoopCompanyGraph()
    for (companyNames <- coops) {
      val start = if (companyNames.length > 6) (companyNames.length - 6) else 0
      for (i <- start until companyNames.length) {
        val companyName = companyNames(i)
        val term = i - start
        coopCompanyGraph.addTerm(companyName, term)     
        for (j <- i + 1 until companyNames.length) {
          val firstCompanyName = companyName
          val firstTerm = term
          val secondCompanyName = companyNames(j) 
          val secondTerm = j - start 
        	coopCompanyGraph.addLink(firstCompanyName, firstTerm, secondCompanyName, secondTerm)
        }
      }
    }
    
    // A map of companies to forwardly-linked companies and the total weight
    // of forward links to each company, represented as a tuple in the form of
    // (company, weight)
    var weightedLinksSeq: Seq[(String, (Set[(String, Int)], Int, Double))] = Seq()
    
    for (company <- coopCompanyGraph.getCompanyMap().values) {
      var forwardLinksSet: Set[(String, Int)] = HashSet()

      var totalWeight = 0
      
      for (forwardCompanyLinks: (String, Buffer[Link]) <- company.forwardLinks) {
        var totalCompanyWeight = 0
        for (forwardLink: Link <- forwardCompanyLinks._2) {
          totalCompanyWeight += forwardLink.weight
        }
        forwardLinksSet += ((forwardCompanyLinks._1, totalCompanyWeight))
        totalWeight += totalCompanyWeight
      }
//      weightedLinksSeq +:= (company.name, (forwardLinksSet, totalWeight, 1))
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
    
//    val links = sc.objectFile[(String, Seq[String])]("links")
//        .partitionBy(new HashPartitioner(100))
//        .persist()
//        
//    var ranks = links.mapValues(v => 1.0)
//    
//    for (i <- 0 until 10) {
//      val contributions = links.join(ranks).flatMap {
//        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
//      }
//      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
//    }
//    
//    ranks.saveAsTextFile("ranks")
  }
  
}