package demo

import model._

trait DemoEntityRepository extends EntityRepository {
  private val securityToIdMap: Map[Security, Int] = Map(
    Equity("apple") -> 1,
    Equity("google") -> 2,
    Bond("usaBondIsin") -> 3,
    Bond("gbrBondIsin") -> 4
  )
  private val securityNameToSecurityMap: Map[String, Security] = {
    securityToIdMap.keys.map(s => s.identity -> s).toMap
  }

  private val portfolioToIdMap: Map[Portfolio, Int] = Map(
    Portfolio("portfolio1") -> 1,
    Portfolio("portfolio2") -> 2
  )
  private val portfolioNameToPortfolioMap: Map[String, Portfolio] = {
    portfolioToIdMap.keys.map(p => p.name -> p).toMap
  }

  override def securityIdOpt(security: Security): Option[Int] = securityToIdMap.get(security)

  override def portfolioIdOpt(portfolio: Portfolio): Option[Int] = portfolioToIdMap.get(portfolio)

  override def securityOpt(securityName: String): Option[Security] = securityNameToSecurityMap.get(securityName)

  override def portfolioOpt(portfolioName: String): Option[Portfolio] = portfolioNameToPortfolioMap.get(portfolioName)
}
