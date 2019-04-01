package framework

import org.squeryl.PrimitiveTypeMode._
import org.squeryl.dsl.ast.LogicalBoolean
import org.squeryl.{Schema, Table}
import utils.{CSVDiagnosticLogger, CSVLoggable, IBasicSquerylAdapter}

import scala.reflect.runtime.universe._

trait SqlTableInsert {
  protected type SchemaType <: Schema
  protected def adapter: IBasicSquerylAdapter
  protected def schema: SchemaType

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

trait OneSqlTableWrite[Rich, Raw] extends TowTruckJob[Raw] with SqlTableInsert {

  def table: Table[Raw]

  protected def stagedData: Seq[Rich]

  protected def serialize(in: Seq[Rich]): Seq[Raw]

  protected def clearCriteria: Raw => LogicalBoolean = clearAll

  final override def prepareData: Iterable[Raw] = serialize(stagedData)

  final override def persist(in: Iterable[Raw]): Unit = insertTable(table, in.toSeq, clearCriteria)

  final override def destinationDescription: String = s"sql table ${table.name}"
}

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