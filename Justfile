default: fmt lint doc test

fmt:
    cargo fmt
    just --fmt --unstable

lint:
    cargo clippy -- \
        -Dclippy::cargo -Dclippy::pedantic -Dclippy::nursery

doc:
    cargo doc

test:
    cargo test

tree:
    cargo tree --edges normal,no-proc-macro \
        --target=x86_64-unknown-linux-gnu
