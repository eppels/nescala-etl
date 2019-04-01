package model

sealed trait Security {
  def identity: String
  override def toString = identity
}

case class Equity(ticker: String) extends Security {
  override def identity: String = ticker
}

case class Bond(isin: String) extends Security {
  override def identity: String = isin
}


