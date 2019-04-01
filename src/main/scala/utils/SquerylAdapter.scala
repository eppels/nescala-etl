package utils

import java.sql.Connection

import org.squeryl.adapters.MSSQLServer
import org.squeryl.{Schema, Session}

trait IBasicSquerylAdapter {
  def createSession(): Session
}

trait BasicDatabaseAdapter {
  def server: String
  def database: String
  def getConnection: Connection
}

trait StubDatabaseAdapter extends BasicDatabaseAdapter {
  override def getConnection: Connection = ???
}

class StubSquerylSqlServerAdapter(override val server: String, override val database: String) extends Schema with StubDatabaseAdapter with IBasicSquerylAdapter {
  override def createSession(): Session = Session.create(getConnection, new MSSQLServer)
}