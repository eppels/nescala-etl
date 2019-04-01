package framework

import utils.Logging

trait ITowTruckApp[T <: TowTruckAppConfig] extends Logging {

  protected def parseArgs(args: Array[String]): Option[T]

  private def run(config: T): Unit = {

    log(s"starting tow truck run in mode: ${config.runMode.name}")

    def runJob(job: TowTruckJob[_]): TowTruckJobResult[_] = TowTruckJobResult(job, job.run())

    val results: Iterable[TowTruckJobResult[_]] = {

      if(config.runMode.runJobsInParallel) {

        config.runMode.jobsToRun
          .groupBy(_.grouperForParallelization.groupingKey)
          .par
          .flatMap { case (_, actionsOnTable) => actionsOnTable.map(runJob) }
          .seq

      } else {

        config.runMode.jobsToRun.map(runJob)

      }
    }

    config.runMode.cleanUp(results)

    val (succeeded, failed) = results.partition(_.success)

    if (failed.nonEmpty) {
      log(s"tow truck did not run all movers successfully. It failed for ${failed.size} out of ${results.size} methods. Printing report")
    } else {
      log("tow truck ran all methods successfully. Printing report")
    }
    failed.map(_.method).foreach { method =>
      log(s"FAILED - ${method.jobName} failed to update ${method.destinationDescription}")
    }
    succeeded.map(_.method).foreach { method =>
      log(s"SUCCEEDED - ${method.jobName} successfully updated ${method.destinationDescription}")
    }
    val exitCode = if (failed.isEmpty) 0 else -100
    log(s"exiting with code $exitCode")
    sys.exit(exitCode)
  }

  def main(args: Array[String]): Unit = {
    try {
      parseArgs(args) match {
        case Some(config) =>
          run(config)
        case None =>
          log("Could not parse args into a config, exiting")
          sys.exit(-2)
      }
    } catch {
      case t: Throwable =>
        log("Unknown error occurred, logging details and exiting")
        logError(t)
        sys.exit(-1)
    }
  }
}
