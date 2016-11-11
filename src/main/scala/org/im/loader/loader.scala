package org.im
package loader

import java.time._, format._
import org.log4s._
import java.time.temporal._
import java.sql._
import com.zaxxer.hikari._
import better.files._
import com.lucidchart.open.relate._
import com.lucidchart.open.relate.Query._
import com.lucidchart.open.relate.interp._
import java.sql.Connection
import com.bizo.mighty.csv._
import au.com.bytecode.opencsv.{ CSVReader => OpenCSVReader }
import java.io.FileReader
import au.com.bytecode.opencsv.CSVParser
import scala.util.{ Try, Success, Failure }
import scala.util.control.Exception._
import cats._
import cats.data._
import cats.implicits._
import Validated.{ Invalid, Valid }
import fs2._
import fs2.io._
import java.util.concurrent.Executors
import scala.concurrent._
import java.util.concurrent.{ atomic => juca }


object Loader {

  private[this] lazy val logger = getLogger

  /** Create a fs2 stream from an iterable. */
  def toStream[A](iter: Iterator[A]) = Stream.unfold(iter) { i => if (i.hasNext) Some((i.next, i)) else None }

  /**
   * A stream that writes a string element to a file.
   * Since this is just a pipe, you need to provide the stream source
   * of strings.
   */
  def fileSink(fileName: String)(implicit s: Strategy) = {
    import java.nio.file.StandardOpenOption
    text.utf8Encode andThen
      fs2.io.file.writeAllAsync[Task](java.nio.file.Paths.get(fileName),
        List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
  }

  /** Flatten a map into a string. */
  def flatten(line: Int, m: Map[String, String]) =
    s"Record $line" + m.map {
      case (k, v) => s"  $k: $v\n"
    }

  /** Insert a block of rows. A commit is run after the block is inserted. */
  def insertChunk(inputs: Seq[(Map[String, String], Int)], specs: mappings, cols: Seq[String],
    baseQuery: InterpolatedQuery,
    errorC: juca.AtomicLong, outputC: juca.AtomicLong,
    getConnection: => java.sql.Connection,
    timeoutInSecs: Int = -1) = {

    val startingRecordIdx = inputs.headOption.map(_._2.toString).getOrElse("unknown position")
    val recordCount = inputs.size
    var con = getConnection
    logger.info(s"Procesing row chunk of size ${recordCount} starting at index $startingRecordIdx")
    require(!con.isClosed)
    inputs.foreach { rowAndIdx =>
      val row = rowAndIdx._1
      val idx = rowAndIdx._2
      logger.debug(s"Processing row ${idx}: $row")
      try {
        val cleanedRow = Implicits.toMapWithOptionValues(row)
        val (values, nas, errors) = specs.transform(cleanedRow, idx)

        // Process notapplicables. Don't do an insert if any.
        if (nas.size > 0) {
          val msg = s"Some targets did not have any rules fire even though rules were defined:\n" +
            nas.map { case (k, v) => s"""Attribute $k: Unused mapping: ${v.map(_.reason.getOrElse("No reason given.")).mkString(",")}""" }.mkString("\n")
          throw new RuntimeException(msg)
        }

        // Process errors. Don't do an insert if any.
        if (errors.size > 0) {
          val errmsg = s"Record ${idx}: Errors in mappings:\n" +
            errors.map { case (k, v) => s"   Attribute $k: ${v.msg}" }.mkString("\n")
          throw new RuntimeException(errmsg)
        }

        // Process valid values. Do an insert!
        val insertValues = Implicits.sequenceLikeUnsafe(cols, specs.load(values))

        val istmt = baseQuery + new InterpolatedQuery("(" + insertValues.map(_ => "?").mkString(",") + ")", insertValues)
        logger.debug("Statement: " + istmt)
        //if (con.isClosed()) con = getConnection
        // how to set timeout?
        val r = istmt.executeInsertSingle { r => s"Record $idx: 1 record inserted." }(con)
      } catch {
        case x: java.sql.SQLException =>
          logger.error(x)(s"SQL processing error near record: ${idx}")
          println(s"An internal processing error occurred at record ${idx}: ${x.getMessage}")
          errorC.incrementAndGet()
        case scala.util.control.NonFatal(e) =>
          logger.error(e)(s"Error processing record ${idx}")
          logger.error(s"Record parsed contents: $row")
          println(s"Error: Record ${idx}: ${e.getMessage}")
          errorC.incrementAndGet()
      }
    }
    try {
      con.commit()
      outputC.addAndGet(recordCount)
      println(s"${program.instantString}: ${specs.table}: $recordCount records committed.")
      con.close()
    } catch {
      case x: java.sql.SQLException =>
        logger.error(x)(s"SQL processing error near $startingRecordIdx.")
        println(s"An internal processing error near $startingRecordIdx: ${x.getMessage}")
        errorC.addAndGet(recordCount)
    }
    recordCount
  }

  /**
   * Not fast or pretty.
   *
   *
   * TODO: Get connection properties from config file or otherwise.
   */
  def load(config: Config) = {

    println("Running load...")

    val ods = new HikariDataSource()
    ods.setUsername(config.username)
    ods.setPassword(config.password)
    ods.setJdbcUrl(config.url)
    config.connectionPoolSize.foreach { s =>
      val default = 2 * config.concurrency
      if (s > default) ods.setMaximumPoolSize(s)
      else ods.setMaximumPoolSize(default)
    }
    //ods.setIdleTimeout(config.idleTimeout)
    ods.setMaximumPoolSize(2 * config.concurrency)
    ods.setInitializationFailFast(true)

    val tpool = Executors.newWorkStealingPool(config.parallelism + 1)
    implicit val ec = scala.concurrent.ExecutionContext.fromExecutor(tpool)
    implicit val strategy = Strategy.fromExecutionContext(ec)
    implicit val sheduler = Scheduler.fromFixedDaemonPool(3, "loader")

    val rows = CSVDictReader2(new OpenCSVReader(new FileReader(config.inputFile)), false)
    val iC = new juca.AtomicLong(0)
    val outputC = new juca.AtomicLong(0)
    val errorC = new juca.AtomicLong(0)

    logger.debug(s"Load using config: $config")
    logger.debug(s"Loading data for entity: ${config.entity}")

    config.mappings.find(_.entity.toLowerCase() == config.entity.toLowerCase()) match {
      case Some(specs) =>

        if (!config.skipSchemaCheck) {
          specs.isValid match {
            case Invalid(msgs) =>
              println("Schema is not valid.")
              msgs.toList.foreach(m => println(s"* $m"))
            case _ =>
          }
        }

        val tname = specs.table.toSql
        val cols = specs.targetCols.sortBy(x => x) // must preserve this order for insert
        val sCols = ("(" + cols.mkString(",") + ")").toSql
        val mappings = specs.mappings
        val schema = specs.schema.map(_ + ".").getOrElse("").toSql

        if (config.clearBeforeInserts) {
          val x = sql"delete from $schema$tname"
          logger.info(s"Issuing delete all records command: $x")
          val affected = x.executeUpdate()(ods.getConnection)
          println(s"Cleared table $tname. Deleted $affected records.")
        }

        type Row = Map[String, String]
        type IRow = (Row, Int)
        val qfragment = sql"""insert into $schema$tname $sCols values"""
        def newCon = { val c = ods.getConnection(); c.setAutoCommit(false); c }

        try {
          val inputs = toStream(rows).covary[Task]
            .map { r => iC.incrementAndGet(); r }
            .zipWithIndex
            .drop { config.drop getOrElse 0 }
            .take { config.take getOrElse Long.MaxValue }
            .filter {
              // run after so the row counting is correct above
              case (Failure(x), idx) =>
                println(s"Parse issue index ${idx + 1}. Continuing.")
                println(s"Error: $x")
                false
              case (Success(row), idx) => true
            }
            .collect {
              case (Success(row), idx) => (row, idx)
            }
            .vectorChunkN(config.commit)

          val loadRowsP: Pipe[Task, Seq[(Map[String, String], Int)], Int] =
            _.evalMap { inputs =>
              Task.delay(insertChunk(inputs, specs, cols, qfragment, errorC, outputC, newCon, config.queryTimeoutInSecs))
            }

          val load = concurrent.join(config.concurrency)(
            inputs.map(rows => Stream.eval(Task.delay(insertChunk(rows, specs, cols, qfragment, errorC, outputC, newCon)))))

          val r = load.run.unsafeAttemptRun match {
            case Right(r) => // Ok
            case Left(t) => throw t
          }
        } finally {
          rows.close
          ods.close
          tpool.shutdown
        }
        println("Record counts:")
        println(s"  Input : ${iC.get}")
        println(s"  Output: ${outputC.get}")
        println(s"  Errors: ${errorC.get}")

        logger.info(specs.report())
        println()
        println(specs.report())

      case _ =>
        println(s"Could not find entity ETL spec for ${config.entity}")
    }
  }

  def exportMappings(config: Config): Unit = {
    import better.files._
    val f = config.exportFile.toFile
    f < "entity,schema,targettable,target,typenote,sources,skipmapping,requiredbytarget,description\n"
    config.mappings.foreach { s =>
      s.allMappings.foreach { m =>        
        val sources = (m.sources ++ (m.source.map(Seq(_)) orElse(Option(Nil))).get).distinct        
        val line =
          s.entity + "," +
            s.schema.getOrElse("") + "," +
            s.table + "," +
            m.target + "," +
            m.typeNote.getOrElse("") + "," +            
            s"""${sources.mkString(";")}""" + "," +
            (if (m.skip) "true" else "false") + "," +
            (if (m.nullable) "false" else "true") + "," +
            m.description.map(_.trim).getOrElse("")
        f << line
      }
    }
  }
}
