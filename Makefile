FLINK = flink-1.10.0

download:
	curl http://apache.mirror.vexxhost.com/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.12.tgz | tar zxf -

start-cluster:
	./${FLINK}/bin/start-cluster.sh

stop-cluster:
	./${FLINK}/bin/stop-cluster.sh

build:
	cd wlc && mvn package

run:
	./flink-1.10.0/bin/flink run wlc/target/wlc-0.1.jar