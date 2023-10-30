target := ./target/debug/pestis_incendium

echo: build
	maelstrom test -w echo --bin ${target} --node-count 1 --time-limit 10

unique-id: build
	maelstrom test -w unique-ids --bin ${target} --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
build: 
	cargo b
