* Download
    http://apache.mirror.vexxhost.com/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.12.tgz
* Follow to start flink daemon
    https://ci.apache.org/projects/flink/flink-docs-stable/getting-started/tutorials/local_setup.html
* Follow to create scala architype
    https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/walkthroughs/datastream_api.html
* Read to understand session windowing
    https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/windowing/SessionWindowing.scala

    mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-walkthrough-datastream-scala -DarchetypeVersion=1.10.0 -DgroupId=wlc -DartifactId=wlc -Dversion=0.1 -Dpackage=spendreport -DinteractiveMode=false

* Download data
    https://github.com/PaytmLabs/WeblogChallenge/blob/master/data/2015_07_22_mktplace_shop_web_log_sample.log.gz?raw=true

