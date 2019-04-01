package model

trait EntityRepository {
  def securityOpt(securityName: String): Option[Security]
  def portfolioOpt(portfolioName: String): Option[Portfolio]
  def securityIdOpt(security: Security): Option[Int]
  def portfolioIdOpt(portfolio: Portfolio): Option[Int]
}
