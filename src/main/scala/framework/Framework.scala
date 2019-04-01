package framework

import org.squeryl.Table
import utils.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.util.{Failure, Success, Try}

/*
The abstract runner requires a runMode, but different applications may have additional configurations
Example additional configurations could be relevant file paths, dates, etc
 */
trait TowTruckAppConfig {
  def runMode: TowTruckRunMode
}

case class TowTruckRunModeConfig(runMode: TowTruckRunMode) extends TowTruckAppConfig

/*
Template for a given run of the app, an app may have several different modes it can run
The minimum information to specify for a given mode is which jobs to run
Can toggle whether the jobs can run in parallel or not - default is that they do
You have an option to make additional code run at the end after all the jobs have completed.
Example clean up actions include closing connections/buffers, running integrity checks that the delivered data is good, etc
 */
abstract class TowTruckRunMode(val name: String) {
  def jobsToRun: Iterable[TowTruckJob[_]]
  def runJobsInParallel: Boolean = true
  def cleanUp(results: Iterable[TowTruckJobResult[_]]): Unit = {}
}

case class TowTruckJobResult[T](job: TowTruckJob[T], success: Boolean)

/*
This is a singular ETL job
A key decision here was to split the extraction from the load, where some collection of type T is passed from extraction to loading
This handles how to run a particular job, but doesn't provide type safety and generally isn't extended directly with a concrete job
Instead, the pattern is to make traits that extend this and provide a more enriched, type-safe interface (JobTemplates)
 */
trait TowTruckJob[T] extends Logging {

  val jobName: String = this.getClass.getSimpleName

  def prepareData: Iterable[T] //the extract, at the level of a concrete job the class constructors provide a way to construct this

  def persist(in: Iterable[T]): Unit //the load

  def destinationDescription: String //this is typically set at the job template level

  def grouperForParallelization: GrouperForParallelization = JobNameGrouper(this)

  //all jobs have a timeout for the extract step, and the default is to retry once with a 50% larger timeout if the extract times out
  def timeoutOverrideInMinutes: Option[Int] = None

  def retrySettings: RetrySettings = RetrySettings(numberRetries = 1, timeoutMultiplier = 1.5)

  //generally jobs do not enforce any particular rules about what universe of data is in the source
  //they just make sense of whatever they can, and throw out any data that can't be modeled
  //generally if the result of an extract is an empty collection, something went wrong and you don't want to try and persist
  def failIfNoDataToPersist: Boolean = true

  private val defaultTimeoutInMinutes: Int = 15

  private val timeoutInMinutes: Int = timeoutOverrideInMinutes.getOrElse(defaultTimeoutInMinutes)

  private object EmptyInputsException extends Throwable

  //key decision is to not let failures in any one job take out the other jobs in a mode
  //so everything here is wrapped in a try and returns a simple success/failure
  def run(): Boolean = {

    log(s"starting $jobName")

    val dataTry: Try[Iterable[T]] = getDataWithAutoRetry(Future(Try(prepareData)))

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

  private def getDataWithAutoRetry(dataFetchTask: Future[Try[Iterable[T]]],
                                   numRetries: Int = retrySettings.numberRetries,
                                   timeout: Double = timeoutInMinutes): Try[Iterable[T]] = {
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

//in some cases will have multiple jobs hit the same sql table, don't want those running in parallel as it can lead to deadlocks
sealed trait GrouperForParallelization {
  def groupingKey: String = this match {
    case SqlTableGrouper(table) => table.name
    case JobNameGrouper(job) => job.jobName
  }
}
final case class SqlTableGrouper(table: Table[_]) extends GrouperForParallelization
final case class JobNameGrouper(job: TowTruckJob[_]) extends GrouperForParallelization