package com.corey.algorithms

import scala.collection.mutable.Buffer
import scala.collection.immutable.HashMap
import scala.io.Source

object GraphBuilder {
  
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
  
  class Link(val firstCompanyName: String, val firstTerm: Int, 
      val secondCompanyName: String, val secondTerm: Int) extends Serializable {
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
  
  def buildCoopCompanyGraph(filename: String): CoopCompanyGraph = {
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
    val coopCompanyGraph = new GraphBuilder.CoopCompanyGraph()
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
    
    coopCompanyGraph
  }
  
//  def buildCoopTermMap(filename: String): 
  
}