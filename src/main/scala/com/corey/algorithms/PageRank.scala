package com.corey.algorithms

import scala.collection.immutable.HashMap
import scala.collection.mutable.Buffer
import scala.io.Source
import scala.collection.immutable.HashSet

object PageRank {

  class Company(val name: String) {
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
  
  class Link(val firstCompanyName: String, val firstTerm: Int, val secondCompanyName: String, val secondTerm: Int) {
    val weight = secondTerm - firstTerm
    
    override def toString(): String = {
      "Link: [firstCompanyName=" + firstCompanyName + ", firstTerm=" + firstTerm  +
          ", secondCompanyName=" + secondCompanyName + ", secondTerm: " + secondTerm + ", weight=" + weight + "]"
    }
  }
  
  class CoopCompanyGraph() {
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
    var weightedLinksMap: scala.collection.mutable.Map[String, (Set[(String, Int)], Int, Double)] = scala.collection.mutable.HashMap()
    
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
        case (companyName, rank) => (companyName, 0.15 + 0.85 * nonZeroRanks.getOrElse(companyName, 0.0))
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