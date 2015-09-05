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

object GraphXPageRank {

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
    var weightedLinksSeq: Seq[(String, String, Int)] = Seq()
    
    for (company <- coopCompanyGraph.getCompanyMap().values) {
      for (forwardCompanyLinks: (String, Buffer[Link]) <- company.forwardLinks) {
        for (forwardLink: Link <- forwardCompanyLinks._2) {
        	weightedLinksSeq +:= (forwardLink.firstCompanyName, forwardLink.secondCompanyName, forwardLink.weight)
        }
      }
    }
    
    val conf = new SparkConf().setAppName("Page Rank Algorithm").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    
    // Create Vertex RDD
    val vertices: RDD[(VertexId, String)] = sc.parallelize(coopCompanyGraph.getCompanyMap().map {
      case (companyName, company) => (companyName.hashCode().toLong, companyName)
    }.toSeq)
    
    // Create Edge RDD
    val edges: RDD[Edge[Int]] = sc.parallelize(weightedLinksSeq).map {
      case (firstCompanyName, secondCompanyName, weight) => {
    	  val firstVertexId: VertexId = firstCompanyName.hashCode.toLong
    	  val secondVertexId: VertexId = secondCompanyName.hashCode.toLong
        Edge(firstVertexId, secondVertexId, weight)
      }
    }
    
//    vertices.foreach(println)
//    edges.foreach(println)
    
    val graph = Graph(vertices, edges)
//    graph.inDegrees.sortBy(_._2, false).foreach(println)
    
    val ranks = graph.pageRank(0.0001).vertices
    
    val ranksByCompany = vertices.join(ranks).map {
      case (id, (companyName, rank)) => (companyName, rank)
    }
    
    println("RANKS BY COMPANY: \n" + ranksByCompany.sortBy(_._2, false).collect().mkString("\n"))
  }
  
}