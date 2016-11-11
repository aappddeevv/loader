package org.im
package loader

import scala.language._
import com.bizo.mighty.csv._
import au.com.bytecode.opencsv.{ CSVReader => OpenCSVReader }
import java.io.{ FileReader, InputStream, InputStreamReader, FileInputStream }
import scala.util.Try
import cats._
import cats.data._

/**
 * Dict reader must have a header. If no header, an exception is thrown immediately.
 */
case class CSVDictReader2(reader: OpenCSVReader, errorIfColumnCountWrong: Boolean = true) extends Iterator[Try[Map[String, String]]] {

  private[this] val rows: Iterator[Row] = new CSVRowIterator(reader) flatten

  val header: Row = {
    if (!rows.hasNext) sys.error("No header found") else rows.next()
  }

  override def hasNext(): Boolean = {
    rows.hasNext
  }

  override def next(): Try[Map[String, String]] =
    Try {
      val currentRow = rows.next()
      if (header.length != currentRow.length && errorIfColumnCountWrong) {
        sys.error("Column mismatch: expected %d-cols. encountered %d-cols\nLine: [[[%s]]]".format(header.length,
          currentRow.length, currentRow.mkString("[-]")))
      } else Map(header.zip(currentRow): _*)
    }

  def close(): Unit = {
    reader.close()
  }
}
