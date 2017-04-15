test:
	@go test -coverprofile cover.out

cover.out:
	@go test -coverprofile $@

coverage: cover.out
	@go tool cover -html=cover.out

.PHONY: coverage test
