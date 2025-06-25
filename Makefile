.PHONY: bench

bench:
	cd opendal && cargo build
	go test -bench=. -benchmem -count=6 -run=^$$ -v

