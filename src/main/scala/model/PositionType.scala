package model

sealed trait PositionType {
  def name: String = this match {
    case HeldPosition => "Held"
    case DesiredPosition => "Desired"
  }
}
object PositionType {
  def parse(name: String): Option[PositionType] = name match {
    case "Held" => Some(HeldPosition)
    case "Desired" => Some(DesiredPosition)
    case _ => None
  }
}

case object HeldPosition extends PositionType
case object DesiredPosition extends PositionType
