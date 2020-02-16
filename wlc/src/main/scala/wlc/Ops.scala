package wlc

import org.scalatest._
import Matchers._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Parser {
  def parse(line: String): (Long, String, String) = {
    try {
      val pattern = "(.{23})[^ ]* [^ ]* ([0-9.]+):.*\"[A-Z]* (http[^ ]*) .*".r
      val pattern(tsText, ip, url) = line

      val tsFmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
      val ts = DateTime.parse(tsText, tsFmt).getMillis
      return (ts, ip, url)
    } catch {
      case e: Exception => {
        return (0, "unknown", "unknown")
      }
    }
  }
}

class ParserSpec extends FlatSpec {
  it should "parse a line" in {
    val line =
      "2015-07-22T09:00:28.050298Z marketpalce-shop 59.183.41.47:62014 10.0.4.227:80 0.000021 0.008161 0.000021 200 200 0 72 \"GET https://paytm.com:443/papi/rr/products/6937770/statistics?channel=web&version=2 HTTP/1.1\""
    val (ts, ip, url) = Parser.parse(line)
    ts should be(1437570028050L)
    ip should be("59.183.41.47")
    url should be(
      "https://paytm.com:443/papi/rr/products/6937770/statistics?channel=web&version=2"
    )
  }
  it should "not crash" in {
    val (ts, ip, url) = Parser.parse("")
    ts should be(0)
    ip should be("unknown")
    url should be("unknown")
  }
}

object Mapper {
  def map(
      state: (Long, Long, Set[String]),
      input: (Long, String, String),
      gapLimit: Long
  ): ((Long, Long, Set[String]), (String, Long, Long, Long)) = {
    // Get current line
    val (inputTimestamp, ip, url) = input

    // Get state
    val (sessionStartTimestamp, lastTimestamp, urlSet) = state

    // Calculate we need to leap
    val leap = if (sessionStartTimestamp == Long.MinValue) {
      true
    } else {
      inputTimestamp - lastTimestamp > gapLimit
    }

    // New state - start
    val newSessionStartTimestamp = if (leap) {
      inputTimestamp
    } else {
      sessionStartTimestamp
    }

    // Counting url
    val (urlCount, newUrlSet) = if (leap) {
      (1L, Set[String](url))
    } else if (urlSet.contains(url)) {
      (0L, urlSet)
    } else {
      (1L, urlSet + url)
    }

    // Session length
    val sessionLength = inputTimestamp - newSessionStartTimestamp

    // Output
    (
      (newSessionStartTimestamp, inputTimestamp, newUrlSet),
      (ip, newSessionStartTimestamp, sessionLength, urlCount)
    )
  }
}

class MapperSpec extends FlatSpec {
  def state0 = { (0L, 50L, Set[String]()) }
  def inputA = { (61L, "ip-a", "url-a") }
  def inputB = { (61L, "ip-a", "url-b") }
  def inputA2 = { (72L, "ip-a", "url-a") }
  def inputB2 = { (72L, "ip-a", "url-b") }
  it should "keep ip" in {
    val (newState, output) = Mapper.map(state0, inputA, 10L)
    output._1 should be("ip-a")
  }
  it should "leap with large gap" in {
    val (newState, output) = Mapper.map(state0, inputA, 10L)
    newState._1 should be(61L)
    newState._2 should be(61L)
    output._2 should be(61L)
    output._3 should be(0L)
  }
  it should "stay with small gap" in {
    val (newState, output) = Mapper.map(state0, inputA, 20L)
    newState._1 should be(0L)
    newState._2 should be(61L)
    output._2 should be(0L)
    output._3 should be(61L)
  }
  it should "count new url" in {
    val (tmpState, _) = Mapper.map(state0, inputA, 20L)
    val (newState, output) = Mapper.map(tmpState, inputB, 20L)
    newState._3.size should be(2)
    output._4 should be(1L)
  }
  it should "discount duplicate url" in {
    val (tmpState, _) = Mapper.map(state0, inputA, 20L)
    val (newState, output) = Mapper.map(tmpState, inputA, 20L)
    newState._3.size should be(1)
    output._4 should be(0L)
  }
  it should "clear set in leap a" in {
    val (tmpState, _) = Mapper.map(state0, inputA, 20L)
    val (newState, output) = Mapper.map(tmpState, inputA2, 10L)
    newState._3.size should be(1)
    output._4 should be(1L)
  }
  it should "clear set in leap b" in {
    val (tmpState, _) = Mapper.map(state0, inputA, 20L)
    val (newState, output) = Mapper.map(tmpState, inputB2, 10L)
    newState._3.size should be(1)
    output._4 should be(1L)
  }
}
