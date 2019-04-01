package utils

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Path

import scala.reflect.runtime.universe._
import scala.collection.mutable
import scala.reflect.ClassTag

// Note - for the demo I did not invest in handling any IO exceptions

trait CSVLoggable[T] {
  def fileName: String
  def header: Seq[String]
  def log(t: T, writeBuffer: BufferedWriter): Unit
  def setSchema(writeBuffer: BufferedWriter): Unit = {
    writeBuffer.write(header.reduce(_ + "," + _) + "\n")
  }
}

object CSVLoggable {
  def fromCaseClass[T <: AnyRef : ClassTag](csvFileName: String): CSVLoggable[T] = {
    val runtimeClass = implicitly[ClassTag[T]].runtimeClass

    new CSVLoggable[T] {
      override def fileName: String = csvFileName

      override def header: Seq[String] = {
        runtimeClass.getDeclaredFields.map { field =>
          field.setAccessible(true)
          field.getName.capitalize
        }.toSeq
      }

      override def log(t: T, writeBuffer: BufferedWriter): Unit = {
        val fieldsMap = t.getClass.getDeclaredFields.map { field =>
          field.setAccessible(true)
          field.getName.capitalize -> field.get(t)
        }.toMap
        val newLine = header.fold("") { (line, field) =>
          line + "," + prettyPrint(fieldsMap(field))
        }
        writeBuffer.write(newLine.stripPrefix(",") + "\n")
      }

      private def prettyPrint(input: Any): String = input match {
        case Some(x) => prettyPrint(x)
        case None => ""
        case _ => input.toString
      }

    }
  }
}

class CSVDiagnosticLogger(fileNames: Seq[String], val directory: Path) {

  directory.toFile.mkdirs()

  private val buffers: Map[String, BufferedWriter] = fileNames.map { fileName =>
    val buffer = new BufferedWriter(new FileWriter(directory.resolve(fileName).toFile))
    fileName -> buffer
  }.toMap

  private val schemaIsSet: mutable.Map[String, Boolean] = mutable.Map() ++ buffers.map { case (fileName, _) => fileName -> false }

  private def setSchemaIfNotSet[T]()(implicit loggable: CSVLoggable[T]): Unit = {
    val fileName = loggable.fileName
    buffers.get(fileName) match {
      case Some(buffer) =>
        if (!schemaIsSet(fileName)) {
          buffer.write(loggable.header.reduce(_ + "," + _) + "\n")
          schemaIsSet += fileName -> true
        }
      case None =>
        throw bufferNotFoundError(fileName)
    }
  }

  def log[T: TypeTag](t: T)(implicit loggable: CSVLoggable[T]): Unit = {
    setSchemaIfNotSet()
    loggable.log(t, buffers(loggable.fileName))
  }

  def logSeq[T: TypeTag](tt: Seq[T])(implicit loggable: CSVLoggable[T]): Unit = {
    tt.foreach(t => log(t))
  }

  def close(): Unit = {
    buffers.foreach { case (_, buffer) =>
      buffer.flush()
      buffer.close()
    }
  }

  private def bufferNotFoundError(fileName: String) = {
    new IllegalStateException(s"Could not set schema for file $fileName, the logger did not have that file in its list")
  }

}
