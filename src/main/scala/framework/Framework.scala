package framework

import org.squeryl.Table
import utils.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success, Try}

trait TowTruckAppConfig {
  def runMode: TowTruckRunMode
}

case class TowTruckRunModeConfig(runMode: TowTruckRunMode) extends TowTruckAppConfig

abstract class TowTruckRunMode(val name: String) {
  def jobsToRun: Iterable[TowTruckJob[_]]
  def runJobsInParallel: Boolean = true
  def cleanUp(results: Iterable[TowTruckJobResult[_]]): Unit = {}
}

case object DefaultTowTruckRunMode extends TowTruckRunMode("Default") {
  override def jobsToRun: Iterable[TowTruckJob[_]] = Seq()
}

case class TowTruckJobResult[Raw](method: TowTruckJob[Raw], success: Boolean)

trait TowTruckJob[Raw] extends Logging {

  val jobName: String = this.getClass.getSimpleName

  def prepareData: Iterable[Raw]

  def persist(in: Iterable[Raw]): Unit

  def destinationDescription: String

  def grouperForParallelization: GrouperForParallelization = JobNameGrouper(this)

  def timeoutOverrideInMinutes: Option[Int] = None

  def failIfNoDataToPersist: Boolean = true

  def retrySettings: RetrySettings = RetrySettings(numberRetries = 1, timeoutMultiplier = 1.5)

  private val defaultTimeoutInMinutes: Int = 15

  private val timeoutInMinutes: Int = timeoutOverrideInMinutes.getOrElse(defaultTimeoutInMinutes)

  private object EmptyInputsException extends Throwable

  def run(): Boolean = {

    log(s"starting $jobName")

    val dataTry: Try[Iterable[Raw]] = getDataWithAutoRetry(Future(Try(prepareData)))

    log(dataTry match {
      case Success(_) => s"finished fetching data for $jobName, now persisting it"
      case Failure(_) => s"failed fetching data for $jobName. Not going to do anything to the destination"
    })

    val persistenceResult = dataTry match {
      case Success(data) if data.isEmpty && failIfNoDataToPersist => Failure(EmptyInputsException)
      case Success(data) => Try(persist(data))
      case f@Failure(_) => f
    }

    persistenceResult match {
      case Success(_) =>
        log(s"done with $jobName")
        true
      case Failure(EmptyInputsException) =>
        log(s"failed to run $jobName. No data to persist and configuration was set to not expect empty data. Did not modify destination")
        false
      case Failure(timeout: TimeoutException) =>
        log(s"failed to run $jobName. Timed out after $timeoutInMinutes minutes. Did not modify destination")
        logError(timeout)
        false
      case Failure(exception) =>
        log(s"failed to run $jobName due to an unknown error. Logging details")
        logError(exception)
        false
    }
  }

  private def getDataWithAutoRetry(dataFetchTask: Future[Try[Iterable[Raw]]],
                                   numRetries: Int = retrySettings.numberRetries,
                                   timeout: Double = timeoutInMinutes): Try[Iterable[Raw]] = {
    try {
      Await.result(dataFetchTask, timeout.minutes)
    } catch {
      case _: TimeoutException if numRetries > 0 =>
        val newTimeout = timeout * retrySettings.timeoutMultiplier
        log(s"Attempted data pull for $jobName timed out after $timeout minutes. Retrying again with timeout $newTimeout minutes")
        getDataWithAutoRetry(dataFetchTask, numRetries - 1, newTimeout)
      case e: Throwable => Failure(e)
    }
  }

}

case class RetrySettings(numberRetries: Int, timeoutMultiplier: Double)

sealed trait GrouperForParallelization {
  def groupingKey: String = this match {
    case SqlTableGrouper(table) => table.name
    case JobNameGrouper(job) => job.jobName
  }
}
final case class SqlTableGrouper(table: Table[_]) extends GrouperForParallelization
final case class JobNameGrouper(job: TowTruckJob[_]) extends GrouperForParallelization