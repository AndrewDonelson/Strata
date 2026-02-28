.PHONY: all test test-unit test-coverage bench load report clean lint help

GO            ?= go
COVEROUT       = coverage.out
BENCHOUT       = reports/bench.txt
LOADOUT        = reports/load.txt
REPORTDIR      = reports

# ── default ───────────────────────────────────────────────────────────────────
all: lint test-coverage bench load report

# ── linting / vetting ─────────────────────────────────────────────────────────
lint:
	$(GO) vet ./...

# ── unit tests (race detector) ────────────────────────────────────────────────
test:
	$(GO) test -race -count=1 ./...

test-unit:
	$(GO) test -race -count=1 -short ./...

# ── coverage ──────────────────────────────────────────────────────────────────
test-coverage:
	$(GO) test -race -count=1 \
	    -coverprofile=$(COVEROUT) \
	    -covermode=atomic \
	    ./...

# ── benchmarks ────────────────────────────────────────────────────────────────
bench: | $(REPORTDIR)
	$(GO) test -run='^$$' -bench=. -benchmem -benchtime=3s ./... \
	    2>&1 | tee $(BENCHOUT)

# ── load tests ────────────────────────────────────────────────────────────────
load: | $(REPORTDIR)
	$(GO) test -race -run='^TestLoad' -v -count=1 -timeout=120s . \
	    2>&1 | tee $(LOADOUT)

# ── HTML + text coverage report ───────────────────────────────────────────────
report: test-coverage | $(REPORTDIR)
	$(GO) tool cover -html=$(COVEROUT) -o $(REPORTDIR)/coverage.html
	$(GO) tool cover -func=$(COVEROUT) | tee $(REPORTDIR)/coverage.txt
	@echo ""
	@echo "Coverage report : $(REPORTDIR)/coverage.html"
	@echo "Coverage summary: $(REPORTDIR)/coverage.txt"
	@echo "Benchmarks      : $(BENCHOUT)"
	@echo "Load tests      : $(LOADOUT)"

# ── integration tests (requires Postgres + Redis) ────────────────────────────
test-integration:
	$(GO) test -race -tags integration -count=1 -timeout=300s ./...

# ── helpers ───────────────────────────────────────────────────────────────────
$(REPORTDIR):
	mkdir -p $(REPORTDIR)

clean:
	rm -f $(COVEROUT)
	rm -rf $(REPORTDIR)

help:
	@echo "Strata test targets:"
	@echo "  make test            – run all tests with race detector"
	@echo "  make test-unit       – run only short tests"
	@echo "  make test-coverage   – run tests + generate coverage.out"
	@echo "  make bench           – run benchmarks (3s each), write reports/bench.txt"
	@echo "  make load            – run load tests, write reports/load.txt"
	@echo "  make report          – run full coverage + produce HTML report"
	@echo "  make lint            – go vet all packages"
	@echo "  make all             – lint + coverage + bench + load + report"
	@echo "  make clean           – remove generated files"
