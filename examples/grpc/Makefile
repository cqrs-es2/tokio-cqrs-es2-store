
all:
	make clean

clean:
	rm -rf target
	rm -rf Cargo.lock
	rm -rf *.log
	rm -rf test.db*

client:
	cargo run --bin client

postgres:
	cargo run --features with-sqlx-postgres --bin server

mysql:
	cargo run --features with-sqlx-mysql --bin server

mariadb:
	cargo run --features with-sqlx-mariadb --bin server

sqlite:
	cargo run --features with-sqlx-sqlite --bin server

mongodb:
	cargo run --features with-mongodb --bin server

redis:
	cargo run --features with-redis --bin server
