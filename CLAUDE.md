# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

`go-ringbufferlib` is a fixed-capacity, concurrency-safe byte ring buffer for Go
(module `github.com/barnowlsnest/go-ringbufferlib`, Go 1.27). It implements
`io.Reader`, `io.Writer` and `io.Closer`, with overwrite or backpressure write
modes, stall timeouts, `context.Context` cancellation and graceful drain on
`Close`.

All code lives in a single package, `pkg/ringbuffer`:

- `ringbuffer.go`: types, options, errors (`ErrTimeout` etc.), constructor, context and timer handling
- `read.go` / `write.go` / `close.go`: the read, write and close paths
- `doc.go`: package documentation (keep in sync with behavior changes)
- `ringbuffer_test.go` / `ringbuffer_bench_test.go`: tests and benchmarks

## Commands

Tasks are defined in `Taskfile.yaml` (CI uses [Task](https://taskfile.dev)):

```sh
task go-build   # go build ./...
task go-test    # go test -v -race -cover ./...
task go-bench   # benchmark smoke run (100x)
task go-vet     # go vet ./...
task go-lint    # golangci-lint run --fix
task sanity     # tidy, fmt, vet, lint, test, bench
```

Without Task: `go test -race -count=3 ./...`, `go vet ./...` and
`golangci-lint run ./...`. Run the tests with `-race`; this is a concurrent
data structure.

## Conventions

- **No runtime dependencies.** The package imports only the standard library.
  `testify` is for tests only.
- **Zero allocations on the hot path.** `Write`/`Read`, including blocked calls
  with a timeout, must not allocate: each buffer creates its two wake-up timers
  once and reuses them. Check benchmarks (`-benchmem`) when touching these paths.
- **Errors are sentinels.** Callers match them with `errors.Is`. `ErrTimeout`
  is a value of an unexported `timeoutError` type that also matches
  `os.ErrDeadlineExceeded`, `os.IsTimeout` and `net.Error.Timeout()`. Keep the
  exported variable typed `error`, and keep `err == ErrTimeout` working.
- **Lint config** is in `.golangci.yaml`: line length 140, `funlen` 100 lines /
  50 statements, `gocyclo` 15, `govet` `fieldalignment` (order struct fields to
  minimize padding), and `goimports` with the module as the local prefix.
  Test files are exempt from several of these.
- **Docs.** Update `README.md` (including the errors table) and `doc.go` when
  public behavior changes. Doc comments use Go doc links such as `[ErrTimeout]`.
- **Tests.** Add a test that fails before a fix and passes after it.

## Workflow

- Branch from `main`; CI (`build.yml`, `lint.yml`) runs on pull requests to `main`.
- Do not edit files under `.github/workflows` from automated sessions.
- Commit messages follow Conventional Commits (`fix:`, `feat:`, `perf:`).
