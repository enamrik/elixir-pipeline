
test_one:
	mix test --only one

test:
	./scripts/restore-deps.sh
	mix test --no-start

.PHONY:test

