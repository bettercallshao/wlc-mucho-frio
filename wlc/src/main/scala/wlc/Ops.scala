package wlc
package wlc

import org.scalatest._
import Matchers._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class Parser {
  def Parse(line: String): (String, Long, String) = {
    return ("",0,"")
  }
}

class ParserSpec extends FlatSpec {
  it should "parse a line" in {
    val line = "2015-07-22T09:00:28.050298Z marketpalce-shop 59.183.41.47:62014 10.0.4.227:80 0.000021 0.008161 0.000021 200 200 0 72 \"GET https://paytm.com:443/papi/rr/products/6937770/statistics?channel=web&version=2 HTTP/1.1\""
    val ts = DateTime.parse("2015-07-22T09:00:28", DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")).getMillis
    1437570028000L should be (ts)
  }
}
