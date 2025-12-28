MAELSTROM_BIN=./bin/maelstrom
MAELSTROM_TEST=$(MAELSTROM_BIN) test -w

.PHONE: build-broadcast

bin:
	wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 -O maelstrom-v0.2.3.tar.bz3

deps:
	brew install gnuplot graphviz

run-unique-id:
	go build -C ./maelstrom-unique-id/ -o ./bin
	$(MAELSTROM_TEST) unique-ids --bin ./maelstrom-unique-id/bin --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition


# Brodcast Tests
build-broadcast:
	go build -C ./maelstrom-broadcast/ -o ./bin
run-broadcast-a: build-broadcast
	$(MAELSTROM_TEST) broadcast --bin ./maelstrom-broadcast/bin --node-count 1 --time-limit 20 --rate 10
run-broadcast-b: build-broadcast
	$(MAELSTROM_TEST) broadcast --bin ./maelstrom-broadcast/bin --node-count 5 --time-limit 20 --rate 10

serve:
	$(MAELSTROM_BIN) serve
