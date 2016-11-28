package org.im
package loader

import org.scalatest._
import cats._
import cats.implicits._
import cats.data._
import Validated._

class miscspec extends FlatSpec with Matchers {

  import cats._
  import cats.data._
  import cats.implicits._
  import Implicits._
  import com.lucidchart.open.relate.interp.Parameter._
  import java.time._
  import java.time.format._
  import DateTimeFormatter._

  val d = "2016-01-01T10:10:10Z"
  val dbad1 = "2016-01-01T01:01:01"
  val f = DateTimeFormatter.ofPattern("u-M-d'T'H:m:s'Z'")

  "date conversion" should "return a Valid with a valid UTC input string" in {
    val tmp = utcConvert(Option(d))
    assert(tmp.isValid)
    assert(tmp.getOrElse(None).isDefined)
    tmp.map {
      _.map { r =>
        val tmp = LocalDate.parse(d, f)
        r.toLocalDate() shouldBe tmp        
      }
    }
  }

  it should "return Invalid with a bad date input and told Invalid is allowed" in {
    val tmp = inputToDate(f, true)(Option(dbad1))
    assert(tmp.isInvalid)
  }

  it should "return None with a bad date input and told not become Invalid" in {
    val tmp = inputToDate(f, false)(Option(dbad1))
    tmp shouldBe None.valid
  }

}
