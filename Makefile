FLINK = flink-1.10.0

download:
	curl http://apache.mirror.vexxhost.com/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.12.tgz | tar zxf -
	curl -L https://github.com/PaytmLabs/WeblogChallenge/blob/master/data/2015_07_22_mktplace_shop_web_log_sample.log.gz?raw=true | gunzip > data.log

start-cluster:
	./${FLINK}/bin/start-cluster.sh

stop-cluster:
	./${FLINK}/bin/stop-cluster.sh

build:
	cd wlc && mvn package

run:
	./flink-1.10.0/bin/flink run wlc/target/wlc-0.1.jar \
	--input $(shell pwd)/data.log \
	--session $(shell pwd)/session.csv \
	--engaged $(shell pwd)/engaged.csv \
	--average $(shell pwd)/average.csv
