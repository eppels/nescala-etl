package demo

import java.nio.file.Path

import framework.OneCsvFileWrite
import model._
import utils.{CSVDiagnosticLogger, CSVLoggable, ConsoleLogging}

import scala.io.Source
import scala.util.Try

/**
  * This job takes as input one csv file of form:
  * PortfolioName,SecurityName,HeldPosition,DesiredPosition
  * And should produce a new csv file of form:
  * PortfolioId,SecurityId,PositionType,Position
  */
case class ModeledPosition(portfolio: Portfolio, security: Security, positionType: PositionType, position: Double)
case class PositionsOutput(portfolioId: Int, securityId: Int, positionType: String, position: Double)

//as mentioned earlier jobs are designed to avoid throwing exceptions, and instead just model whatever it can and throw away what it can't
//this can be a concern at both extraction and loading step as demonstrated here
class MovePositionsJob(sourceFile: Path,
                       override protected val csvPersister: CSVDiagnosticLogger)
                      (implicit domain: EntityRepository,
                       evL: CSVLoggable[PositionsOutput])
  extends OneCsvFileWrite[ModeledPosition, PositionsOutput] with ConsoleLogging {

  override protected def stagedData: Iterable[ModeledPosition] = {
    Source.fromFile(sourceFile.toFile).getLines().toIndexedSeq.tail.flatMap { line =>
      val parts = line.split(',')
      if (parts.length != 4) {
        Seq()
      } else {
        parseLine(parts(0), parts(1), parts(2), parts(3))
      }
    }
  }

  private def parseLine(portfolioName: String, securityName: String, heldPositionText: String, desiredPositionText: String): Seq[ModeledPosition] = {
    (for {
      portfolio <- domain.portfolioOpt(portfolioName).toSeq
      security <- domain.securityOpt(securityName).toSeq
      heldPosition <- Try(heldPositionText.toDouble).toOption.toSeq
      desiredPosition <- Try(desiredPositionText.toDouble).toOption.toSeq
    } yield Seq(
      ModeledPosition(portfolio, security, HeldPosition, heldPosition),
      ModeledPosition(portfolio, security, DesiredPosition, desiredPosition)
    )).flatten
  }

  override protected def serialize(in: Iterable[ModeledPosition]): Iterable[PositionsOutput] = {
    in.flatMap { position =>
      domain.portfolioIdOpt(position.portfolio).flatMap { portfolioId =>
        domain.securityIdOpt(position.security).map { securityId =>
          PositionsOutput(portfolioId, securityId, position.positionType.name, position.position)
        }
      }
    }
  }
}