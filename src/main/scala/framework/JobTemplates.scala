package framework

import org.squeryl.PrimitiveTypeMode._
import org.squeryl.dsl.ast.LogicalBoolean
import org.squeryl.{Schema, Table}
import utils.{CSVDiagnosticLogger, CSVLoggable, IBasicSquerylAdapter}

import scala.reflect.runtime.universe._

/*
The two example templates shown here are for writing to sql and writing to csv
The sql one is a bit under-abstracted to be specific to using squeryl as the ORM, haven't had a use case for a different one come up yet
 */
trait SqlTableInsert {
  protected type SchemaType <: Schema
  protected def adapter: IBasicSquerylAdapter
  protected def schema: SchemaType

  //haven't found better squeryl syntax for handling always true / always false cases
  protected def clearAll[T]: T => LogicalBoolean = x => 1 === 1
  protected def clearNone[T]: T => LogicalBoolean = x => 1 === 2

  protected def insertTable[Dto](table: Table[Dto], rows: Seq[Dto], clearCriteria: Dto => LogicalBoolean = clearAll): Unit = {
    transaction(adapter.createSession()) {
      table.deleteWhere(clearCriteria)
      table.insert(rows)
    }
  }
}

trait WriteCsv {

  protected def csvPersister: CSVDiagnosticLogger

  protected def writeCsv[Dto: TypeTag](rows: Seq[Dto])(implicit ev: CSVLoggable[Dto]): Unit = {
    csvPersister.logSeq(rows)
  }
}

//this is actually the most commonly implemented template we have
//it ties the raw type to the type of the table, which provides compile time validation that you are writing consistently w/ the sql schema
//the downstream interface now just asks you to come up with data of one type and specify how to serialize it
trait OneSqlTableWrite[Rich, Raw] extends TowTruckJob[Raw] with SqlTableInsert {

  def table: Table[Raw]

  protected def stagedData: Seq[Rich]

  protected def serialize(in: Seq[Rich]): Seq[Raw]

  //most common behavior is to wipe and load the destination which is why it is the default
  protected def clearCriteria: Raw => LogicalBoolean = clearAll

  final override def grouperForParallelization: GrouperForParallelization = SqlTableGrouper(table)

  final override def prepareData: Iterable[Raw] = serialize(stagedData)

  final override def persist(in: Iterable[Raw]): Unit = insertTable(table, in.toSeq, clearCriteria)

  final override def destinationDescription: String = s"sql table ${table.name}"
}

//the csv case is very similar - it provides the same external interface except instead of Table[Raw] you have CSVLoggable[Raw]
//there's no notion of clear criteria here - expected you are always creating new files instead of appending to existing ones
abstract class OneCsvFileWrite[Rich, Raw: TypeTag](implicit ev: CSVLoggable[Raw]) extends TowTruckJob[Raw] with WriteCsv {

  protected def stagedData: Iterable[Rich]

  protected def serialize(in: Iterable[Rich]): Iterable[Raw]

  final override def prepareData: Iterable[Raw] = {
    serialize(stagedData)
  }

  final override def persist(in: Iterable[Raw]): Unit = {
    writeCsv(in.toSeq)
  }

  final override def destinationDescription: String = s"csv file ${ev.fileName}"
}