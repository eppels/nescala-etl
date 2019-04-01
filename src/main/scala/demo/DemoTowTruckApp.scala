package demo

import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import framework._
import utils.{CSVDiagnosticLogger, CSVLoggable, ConsoleLogging}

object DemoTowTruckApp extends ITowTruckApp[TowTruckRunModeConfig] with ConsoleLogging {

  override protected def parseArgs(args: Array[String]): Option[TowTruckRunModeConfig] = {
    runModes.find(_.name == args(0)).map(TowTruckRunModeConfig)
  }

  private val runModes: Seq[TowTruckRunMode] = {
    Seq(
      new TowTruckRunMode("MovePositions") {

        val sourceFile = Paths.get("/Users", "max", "demo", "raw", "input.csv")
        val destinationFolder = Paths.get("/Users", "max", "demo", "processed")
        val desitnationName = s"output_${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}.csv"

        val csvPersister = new CSVDiagnosticLogger(Seq(desitnationName), destinationFolder)
        implicit val evL = CSVLoggable.fromCaseClass[PositionsOutput](desitnationName)
        implicit val domain = new DemoTowTruckDomain

        override def jobsToRun: Iterable[TowTruckJob[_]] = Seq[TowTruckJob[_]](
          new MovePositionsJob(sourceFile, csvPersister)
        )

        override def cleanUp(results: Iterable[TowTruckJobResult[_]]): Unit = {
          csvPersister.close()
        }
      }
    )
  }

}
