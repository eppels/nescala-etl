package utils

trait Logging {
  def log(message: String): Unit
  def logError(error: Throwable): Unit = {
    log(s"Error occurred. Message: ${error.getMessage}\nStack trace: ${error.getStackTrace.foreach(println)}")
  }
}

trait ConsoleLogging extends Logging {
  override def log(message: String): Unit = println(message)
}