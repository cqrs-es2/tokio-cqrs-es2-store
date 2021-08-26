
all:
	make clean
	make build

clean:
	rm -rf target
	rm -rf Cargo.lock
	rm -rf *.log
	rm -rf demo.db*

build:
	cargo build

test:
	cargo test

run:
	cargo run --bin server
