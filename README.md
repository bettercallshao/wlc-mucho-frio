# Weblog Challenge

## Demo

Run on a Linux box with Java 1.8, maven, and make, run the following in order.

    make download
    make start-cluster
    make build
    make run
    make stop-cluster

Or if you are brave.

    make download start-cluster build run stop-cluster

## Data Exploration

Some simple python plots are made in `explore.ipynb` to validate the ordering of log entires. The conclusion is entries are sometimes out of order by only by small time differences, and we should treat this stream as ordered.

## Tool Selection

This challenge can be thought of as either a batch problem or a streaming problem. Since I have been less practiced on streaming, I took this chance to learn about Flink.

Based on the tutorial, flink 1.10 for scala 2.11 is downloaded and bootstrapped with.

    mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-scala \
    -DarchetypeVersion=1.10.0 -DgroupId=wlc -DartifactId=wlc \
    -Dversion=0.1 -Dpackage=spendreport \
    -DinteractiveMode=false

## Assumptions

* Timestamp and durations are represented as milliseconds (since epoch).
* URLs are parsed to include protocol and parameters, excluding http methods and browser info.
* Sessions shorter than 1 second is considered abnormal and discarded. (~10% of the sessions)
* Most engaged user is ranked by cumulative session time (adding all sessions together) instead of average session time. It is a design decision, either can be accomplished.
* Top 10 most engaged users are calculated.
* Watermark is set to lag event time by 3 seconds.
* Inactivity gap for session breaking is set to 15 minutes.

## Time Windowing

In a streaming scenario, aggregations can only be done with a time window, which should be chosen to reflect business logic. But here since we don't want to produced results sliced by time, it is chosen to be arbitrarily large. (120 hours)

The ideal approach is to use Flink's own session windowing mechanism (`EventTimeSessionWindows.withGap`), but I couldn't get it to work properly.

## Outputs

The processing creates three outputs

1. `session.csv`: IP, SessionStartTime, SessionLength, SessionUniqueUrls.
2. `average.csv`: AverageSessionLength.
3. `engaged.csv`: Constant, IP, CumulativeSessionLength. Top 10 most engaged in descending order.

## Apporach

Flink logic is written in `Wlc.scala`.

First output.

1. Souce is created from reading a local file.
2. Each line is parsed to yield (Timestamp, IP, URL).
3. Timestamp is assigned to the Flink element.
4. Session is keyed by IP and sessionized to create (IP, SessionStartTime, SessionLength, UrlCount (1 if url is new, 0 if url has been counted). A stateful mapper is used.
5. Reduce to find maximum of SessionLength and sum of UrlCount.
6. Filter out sessions with SessionLength less than 1 second.

Second output from first.

1. Reduce to find NumberOfSessions and sum of SessionLength.
2. Divide both to produce average.

Third output from first.

1. Keyed by IP to find sum of SessionLength.
2. Apply TopK aggregator to find IPs with largest sum.

Operations / helpers are written in `Ops.scala`, unit tests are included.

1. Parser for log lines.
2. Mapper for sessionization.

## Improvements

* Preliminary validation is done against the exploration results, but in depth validation from other source should be done before we can trust the pipeline.
* Inactivity gap, session cut off, and watermark lag can be tuned based on analysis of result and business needs.
* Utilize user agent data (os, type, version) in addition to IP to identify different users.
* Flink's `keyBy` is used redundantly a few times. More research may yield a workaround.
* Flink's `timeWindow` is used repeatedly over the same stream. More research may yield a workaround.
* If Flink's session windowing is not going to be used, it is redundant to assign event time and watermark.
* Find more scalable ways to implement the custom algorithms including deduplicatioin of URLs and TopK.  

## References

https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/walkthroughs/datastream_api.html

https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/

https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#global-windows

https://ci.apache.org/projects/flink/flink-docs-stable/getting-started/tutorials/local_setup.html

https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/windowing/SessionWindowing.scala

https://scastie.scala-lang.org/

https://www.scala-lang.org/api/current/scala/collection/mutable/PriorityQueue.html
