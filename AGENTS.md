# AGENTS.md

## Project and scope

dacfy is a Go CLI for managing ClickHouse data pipelines defined in YAML.
These instructions apply throughout this repository. Follow more specific
instructions in nested `AGENTS.md` files for their directories and the user's
explicit directions for the current task.

## Working agreement

- Read this file before making changes.
- Keep changes small and scoped to the request. Do not refactor unrelated code.
- Explain a meaningful assumption or tradeoff before acting when it affects the design.
- Proceed with routine implementation decisions within the approved scope. Ask
  only when a missing decision materially changes the scope or expected behavior.
- Do not commit, push, open a pull request, or close an issue unless the user asks.
- Use `apply_patch` for source and documentation edits.
- Preserve existing uncommitted changes. Do not reset, overwrite, or stash them
  without the user's instruction.
- Format changed Go files with `gofmt` and run the relevant tests. Before handing
  off a Go feature or fix, run `go test ./...`, `go vet ./...`, and
  `git diff --check`. Report any check that could not run and why. Documentation
  changes need content, link, and whitespace checks, not the Go test suite.

## Engineering principles

Apply principles in their numbered order when they conflict. Principles 1 to 4
take precedence over principles 5 to 15; patterns and OOP never justify work
outside the requested scope.

1. Do not add logic or functionality that was not requested. Do not add extra
   features, options, fallbacks, endpoints, or "while I'm here" behavior. If a
   change beyond the requested outcome appears necessary, propose it and wait
   for approval. Normal implementation steps required for that outcome are
   already in scope.
2. Use a design pattern only when it solves a concrete problem in the task. Name
   the pattern in the implementation explanation when using constructor/factory,
   strategy, adapter, decorator, repository, functional options, or another
   pattern. A named pattern is not a requirement for every change.
3. Follow software development best practices: single responsibility, small
   functions, clear names, dependency injection through constructors instead of
   globals, contextual errors with `fmt.Errorf("...: %w", err)`, and
   `context.Context` as the first parameter where I/O or cancellation applies.
4. Use Go's equivalent of OOP where it fits: model domain concepts as structs
   with their own methods; construct them with `NewX(...)`; keep fields
   unexported unless callers need them; and keep data and behavior together. Put
   behavior on the type instead of in free functions that receive that type as a
   parameter. Prefer concrete domain types to generic frameworks, but do not
   create types that only wrap trivial functions or repeated data. Use value
   receivers for small immutable types and pointer receivers otherwise; do not
   mix receiver kinds on one type. Avoid package-level mutable state.
5. Prefer the smallest change that solves the problem. If 200 lines can be 50,
   rewrite it.
6. Do not introduce abstractions for hypothetical future requirements. Three
   similar lines are preferable to a premature abstraction.
7. Avoid duplicate code: reuse existing patterns and extract shared code only
   when it has more than one real use.
8. Prefer existing patterns over new architecture. Before adding a type, package,
   or interface, explain the concrete problem it solves.
9. Do not add defensive checks or error handling for states that are impossible
   by construction or database constraints.
10. Let unexpected errors propagate to the existing error boundary. Handle an
    error only where this layer can meaningfully recover from it. Checking and
    returning an error, wrapping it with context, and releasing resources are
    propagation and cleanup, not recovery. Avoid logging and returning the same
    error at multiple layers.
11. Prefer the Go standard library and idiomatic features over custom helpers or
    new dependencies when they solve the problem clearly.
12. Prefer composition through fields or embedding over inheritance-style
    abstractions. Introduce interfaces only for multiple real implementations or
    a concrete boundary to protect, and define them where they are consumed.
13. Keep queries, parsing, and transformations explicit and easy to audit. Avoid
    hidden execution paths and magic configuration.
14. Organize code into small cohesive packages around domain responsibilities.
    Prefer one primary domain struct per file with its methods, constructor,
    parsing, and helpers. Split files by domain concept, not by size.
15. Optimize for the readability of the code that remains, not theoretical
    extensibility.

Before adding code, check internally: was it explicitly requested; is it required
by the specification; does the repository already solve it; is this state
possible; is the error recoverable here; does the abstraction have more than one
real use; and can it be simpler?

## Feature and fix workflow

When the user asks to implement a feature or a fix:

1. Inspect the current branch and worktree. Use an issue supplied by the user or
   already associated with the current task. Otherwise, check for an appropriate
   existing issue; create one if none exists, describing the problem, expected
   behavior, scope, and acceptance criteria. The implementation request authorizes
   this issue creation; do not ask for the same approval again.
2. Create or switch to `issue-<id>`, where `<id>` is the GitHub issue number,
   before editing implementation files. Reuse that branch when it already exists
   and belongs to this task. Do not carry unrelated pending changes to a different
   issue; use an isolated worktree when practical or ask if the changes prevent
   proceeding safely. Never force a branch switch or discard changes.
3. Implement and test the change on that branch.

Do not create a duplicate issue when an appropriate existing issue is already in
scope. A request limited to analysis, review, documentation, explanation, or a
commit message does not require a new issue or branch. If the user requests only
an issue, create only the issue. Continue work on the same issue and branch for
follow-ups within its scope. If GitHub access is unavailable, report the blocker
before implementation instead of inventing an issue number.

Use the available GitHub integration or `gh`. For multiline issue bodies, write
the content to a temporary file with `apply_patch` and pass it with `--body-file`;
do not embed Markdown with backticks inside a shell double-quoted argument.

## Go design and style

- Follow the engineering principles above first. Match existing repository
  conventions only when they do not conflict with them.
- Constructors initialize runtime domain objects and their dependencies. YAML
  models may expose fields for decoding; do not add empty constructors solely to
  wrap a struct literal. Refactor existing globals only within the requested scope.
- Keep methods focused, aiming for roughly 30 lines or fewer. Split by meaningful
  domain operations rather than mechanically meeting a line count. Put shared
  state on its owning struct instead of repeatedly passing it between methods;
  keep one-use temporary values local. A receiver should use the state it owns.

## Tests and data validation

- The `dacfy test` command validates existing ClickHouse data. It must not create
  tables, fixtures, or other data to execute a manifest. This restriction does not
  prevent isolated mocks or in-memory fixtures in Go unit tests.
- Keep the test command output concise: test name, identifier or rule, and PASS or
  FAIL with a short detail.
- Preserve manifest order when reporting expected-row results.
- Treat `NULL` distinctly from the literal string `"<null>"`.
- When extending test modes, validate the YAML shape before generating SQL and add
  focused unit tests for valid behavior and relevant failures.
- Apply `--suffix` to the test table name without mutating the parsed manifest.
- Test observable behavior and relevant failures. For a bug fix, add a regression
  test when feasible. Avoid tests that merely mirror implementation details.

## Documentation

- Put detailed command documentation in `docs/`; keep `README.md` as an entry
  point with links to those documents.
- Update documentation and examples when a user-facing YAML field, flag, output,
  or behavior changes.
