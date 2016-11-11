package org.im
package loader
package csv

import java.time._, format._
import org.log4s._
import java.time._
import java.time.temporal._
import java.sql._
import com.zaxxer.hikari._
import better.files._
import com.lucidchart.open.relate._
import com.lucidchart.open.relate.interp._
import java.sql.Connection

case class Config(
  mode: String = "load",
  /** JDBC url. */
  url: String = "",
  timeout: Int = 30 * 60, // seconds
  username: String = "",
  password: String = "",
  /** The file to write a summary of the mappings to, csv format. */
  exportFile: String = "mapping",
  /** Input file to read data from */
  inputFile: String = "",
  /** Tag on the mappings to load. */
  entity: String = "",
  clearBeforeInserts: Boolean = false,
  take: Option[Long] = None,
  drop: Option[Long] = None,
  commit: Int = 10000,
  skipSchemaCheck: Boolean = false,
  concurrency: Int = 2,
  parallelism: Int = 10,
  connectionPoolSize: Option[Int] = None,
  idleTimeout: Long = 10 * 60 * 1000,
  queryTimeoutInSecs: Int = 0,
  /** Provide your list of mappings here and re-use the main class as much as possible. */
  mappings: Seq[mappings] = Seq(),
  /** Provide other parameters your derivative of the loader needs to have. */
  other: Map[String, String] = Map())

object program {

  private[this] implicit val logger = getLogger

  val defaultConfig = Config()

  val parser = new scopt.OptionParser[Config]("loader") {
    override def showUsageOnError = true
    opt[String]('u', "username").optional().valueName("<username>").text("username")
      .action((x, c) => c.copy(username = x))
    opt[String]('p', "password").optional().valueName("<password>").text("password")
      .action((x, c) => c.copy(password = x))
    opt[String]('r', "url").optional().valueName("<url>").text("JDBC url.").
      action((x, c) => c.copy(url = x))
    opt[Int]("parallelism").text("Parallelism.").
      validate(con =>
        if (con < 1 || con > 32) failure("Parallelism must be between 1 and 32")
        else success).
      action((x, c) => c.copy(parallelism = x))
    opt[Int]("concurrency").text("Concurrency factor.").
      validate(con =>
        if (con < 1 || con > 16) failure("Concurrency must be between 1 and 32")
        else success).
      action((x, c) => c.copy(concurrency = x))
    opt[Int]("connection-pool-size").text("JDBC connection pool size.").
      action((x, c) => c.copy(connectionPoolSize = Some(x)))

    help("help").text("Show help")

    cmd("load").action((_, c) => c.copy(mode = "load")).
      text("Load a CSV file into a target RDBMS table.").
      children(
        opt[String]("input").valueName("<filename>").text(s"Input CSV filename.").
          action((x, c) => c.copy(inputFile = x)),
        opt[String]("entity").valueName("<entity name to load>").text("Entity name. Maps to predefined ETL mappings.").
          action((x, c) => c.copy(entity = x)),
        opt[Unit]("clear").text("Clear table of all rows before insert.").
          action((x, c) => c.copy(clearBeforeInserts = true)),
        opt[Int]("take").text("Process only the top N rows (take)").
          action((x, c) => c.copy(take = Some(x))),
        opt[Int]("commit").text("Rows before commit.").
          action((x, c) => c.copy(commit = x)),
        opt[Unit]("skip-schema-check").text("Do not check the schema before loading.").
          action((x, c) => c.copy(skipSchemaCheck = true)),
        opt[Int]("drop").text("Skip records before loading, not including header line. Drop applied before take.").
          action((x, c) => c.copy(drop = Some(x))),
        opt[Long]("idle-timeout").text("Timeout on keeping connections hot in the pool. If larger and more records in commit, set this larger.")
          action ((x, c) => c.copy(idleTimeout = x)),
        opt[Int]("query-timeout").text("Timeout in seconds on an query. You probably should change this instead of idle-timeout. Default is no time-out.")
          action ((x, c) => c.copy(queryTimeoutInSecs = x)),
        opt[String]("mappings").text("Export mappings to file in CSV format suitable to load into Excel.").
          action((x, c) => c.copy(mode = "printMappings", exportFile = x)))

    note("The url should be a JDBC url. See your JDBC provider for formats and details.")

    /**
     * The options from this parser to be rolled into another. Or just
     *  copy the options from above.
     */
    def stdargs = options
  }

  /**
   * For most people, the main entry point into the loader.
   *  You have a chance to modify your mappings e.g. perform
   *  initialization, using `f`.
   */
  def runloader(args: scala.Array[String], parser: scopt.OptionParser[Config],
    defaultConfig: Config, f: Config => Config): Unit = {
    val config = parser.parse(args, defaultConfig) match {
      case Some(c) => c
      case None => return
    }
    val start = Instant.now
    println(s"Program start: ${instantString}")
    try {
      config.mode match {
        case "load" => Loader.load(f(config))
        case "printMappings" => Loader.exportMappings(f(config))
        case x@_ => println(s"Unknown mode: $x")
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.error(e)("Error during processing.")
        println(s"Error during processing: ${e.getMessage}.")
        println("See the application log file.")
    }

    val stop = Instant.now
    println(s"Program stop: ${instantString}")
    val elapsed = Duration.between(start, stop)
    println(s"Program runtime in minutes: ${elapsed.toMinutes}")
  }

  def instantString = {
    val formatter =
      DateTimeFormatter.ofLocalizedDateTime(FormatStyle.LONG)
        .withZone(ZoneId.systemDefault());
    formatter.format(Instant.now)
  }

  /**
   * Write your own main, this is here as a reference.
   */
  def main(args: scala.Array[String]): Unit = {
    runloader(args, parser, defaultConfig, (c: Config) => c)
  }

}
