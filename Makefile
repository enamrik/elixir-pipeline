
test_one:
	mix test --only one

tests:
	./scripts/restore-deps.sh
	mix test --no-start

