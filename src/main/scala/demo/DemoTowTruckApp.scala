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

        //todo - point the below to the right location on your machine if you are trying to run this yourself
        //if you want to try getting your hands dirty w/ this framework, one of the extensions is to take these in as command line args
        val sourceFile = Paths.get("/Users", "max", "demo", "raw", "input.csv")
        val destinationFolder = Paths.get("/Users", "max", "demo", "processed")
        val desitnationName = s"output_${LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))}.csv"

        val csvPersister = new CSVDiagnosticLogger(Seq(desitnationName), destinationFolder)
        implicit val evL = CSVLoggable.fromCaseClass[PositionsOutput](desitnationName)
        implicit val domain = new DemoTowTruckDomain

        override def jobsToRun: Iterable[TowTruckJob[_]] = Seq[TowTruckJob[_]](
          new MovePositionsJob(sourceFile, csvPersister)
        )

        //here's an example of using cleanUp to flush & close the write buffers, as you may use the same csv persister for multiple jobs
        override def cleanUp(results: Iterable[TowTruckJobResult[_]]): Unit = {
          csvPersister.close()
        }
      }
    )
  }

}
