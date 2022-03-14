# insert data in postgres from a csv file with a cron job

$ rustup update stable
$ rustup --version
$ cargo new rust_csv --bin

$ sudo -u postgres psql
CREATE DATABASE rust_csv WITH OWNER test;
DROP DATABASE rust_csv;

## run rust
$ cargo run
