PKG		= exec
SQLFILES	= $(wildcard sql/*.sql)

test:
	@go test -coverprofile cover.out

cover.out:
	@go test -coverprofile $@

coverage: cover.out
	@go tool cover -html=cover.out

sql/bindata.go: $(SQLFILES)
	@cd sql && go-bindata -pkg sql .

.PHONY: coverage test
