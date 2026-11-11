# AGENTS.md

## Project and scope

dacfy is a Go command-line tool that manages ClickHouse data pipelines written
in YAML. These rules apply to the whole repository. A nested `AGENTS.md` file
adds rules for its own directory. The user's direct instructions for the
current task come first.

## Working rules

- Read this file before you change anything.
- Keep changes small and limited to the request. Do not refactor code that is
  not part of the request.
- If an assumption or a tradeoff changes the design, explain it before you act.
- Make routine implementation decisions yourself when they are inside the
  approved scope. Ask only when a missing decision changes the scope or the
  expected behavior.
- Do not commit, push, open a pull request, or close an issue unless the user
  asks. A request to implement a feature or a fix counts as that request:
  follow the feature and fix workflow below until the pull request is open.
- Use `apply_patch` to edit source code and documentation.
- Keep uncommitted changes that already exist. Do not reset, overwrite, or
  stash them unless the user tells you to.
- Run `gofmt` on changed Go files and run the related tests. Before you hand
  off a Go feature or fix, run `go test ./...`, `go vet ./...`, and
  `git diff --check`. If a check cannot run, say which one and why. A
  documentation change needs content, link, and whitespace checks, not the Go
  tests.

## Engineering principles

When two principles conflict, follow the one with the lower number. Principles
1 to 4 come before principles 5 to 15. Design patterns and OOP are never a
reason to work outside the requested scope.

1. Do not add logic or features that were not requested. This includes extra
   options, fallbacks, endpoints, and small "while I'm here" changes. If a
   change outside the request seems necessary, propose it and wait for
   approval. The normal steps needed to deliver the request are already in
   scope.
2. Use a design pattern only when it solves a real problem in the task. When
   you use one, name it when you explain the change: for example
   constructor/factory, strategy, adapter, decorator, repository, or
   functional options. A change does not need a named pattern.
3. Follow good development practices: one responsibility per unit, small
   functions, and clear names. Pass dependencies through constructors, not
   globals. Add context to errors with `fmt.Errorf("...: %w", err)`. Make
   `context.Context` the first parameter when there is I/O or cancellation.
4. Use Go's version of OOP where it fits. Model domain concepts as structs with
   their own methods, and build them with `NewX(...)`. Keep fields unexported
   unless callers need them. Keep data and behavior together: put behavior on
   the type, not in free functions that take the type as a parameter. Prefer
   concrete domain types to generic frameworks, but do not create types that
   only wrap a trivial function or repeated data. Use value receivers for small
   immutable types and pointer receivers for the rest. Do not mix both kinds on
   one type. Avoid mutable package-level state.
5. Make the smallest change that solves the problem. If 200 lines can be 50,
   rewrite them.
6. Do not add abstractions for future needs that may never come. Three similar
   lines are better than an early abstraction.
7. Avoid duplicate code. Reuse existing patterns, and extract shared code only
   when it has more than one real use.
8. Prefer existing patterns to new architecture. Before you add a type, a
   package, or an interface, explain the real problem it solves.
9. Do not add checks or error handling for states that cannot happen because of
   how the code is built or because of database constraints.
10. Let unexpected errors go up to the existing error boundary. Handle an error
    only where this layer can really recover from it. Checking and returning an
    error, adding context to it, and releasing resources are not recovery. Do
    not log and return the same error at more than one layer.
11. Prefer the Go standard library and normal Go features to custom helpers or
    new dependencies when they solve the problem clearly.
12. Prefer composition, with fields or embedding, to inheritance-style
    abstractions. Add an interface only when there are several real
    implementations or a real boundary to protect. Define the interface where
    it is used.
13. Keep queries, parsing, and transformations explicit and easy to review.
    Avoid hidden execution paths and magic configuration.
14. Organize code in small packages, each with one domain responsibility. Prefer
    one main domain struct per file, with its methods, constructor, parsing, and
    helpers. Split files by domain concept, not by size.
15. Write for the readability of the code that stays, not for imagined future
    extensions.

Before you add code, ask yourself: Was it requested? Does the specification
need it? Does the repository already do it? Can this state really happen? Can
this layer recover from the error? Does the abstraction have more than one real
use? Can it be simpler?

## Feature and fix workflow

When the user asks you to implement a feature or a fix:

1. Check the current branch and worktree. Use the issue the user gives you or
   the issue already linked to the task. If there is none, look for a matching
   open issue. If none exists, create one with the problem, the expected
   behavior, the scope, and the acceptance criteria. The implementation request
   already allows you to create the issue; do not ask again.
2. Before you edit implementation files, create or switch to the branch
   `issue-<id>`, where `<id>` is the GitHub issue number. If that branch
   already exists for this task, reuse it. Do not carry unrelated pending
   changes to another issue: use a separate worktree when you can, or ask if
   the changes stop you from going on safely. Never force a branch switch or
   throw away changes.
3. Look at how earlier issues, pull requests, commits, code, and tests were
   written, and follow the same style.
4. Implement and test the change on that branch. Update the documentation in
   `docs/` and `README.md` when users can see the change.
5. Commit, push the branch, and open a pull request against `main` that closes
   the issue. Each change has its own issue, branch, and pull request. Do not
   mix unrelated changes.

Writing style and authorship:

- Write issues, pull requests, commit messages, code comments, and
  documentation in simple English.
- An issue body has three sections: `## Problem`,
  `## Expected behavior and scope`, and `## Acceptance criteria`.
- Issue, commit, and pull request titles use the form `type(scope): summary`,
  for example:
  `feat(backfill): add --reverse to run chunks from newest to oldest`.
  The pull request uses the commit title and a short body that starts with
  `Closes #<id>`.
- Commits and pull requests are in the repository owner's name. Do not add AI
  attribution, such as `Co-Authored-By` lines or "Generated with" footers.
- Do not put secrets, credentials, internal hostnames, or IP addresses in
  issues or pull requests. Use placeholders instead.

Do not create a second issue when a matching issue already exists. A request
that is only analysis, review, documentation, explanation, or a commit message
does not need a new issue or branch. If the user asks only for an issue, create
only the issue. Keep follow-up work for the same scope on the same issue and
branch. If you cannot reach GitHub, report it before you start; never invent an
issue number.

Use the available GitHub integration or `gh`. For an issue body with several
lines, write it to a temporary file with `apply_patch` and pass it with
`--body-file`. Do not put Markdown with backticks inside a double-quoted shell
argument.

## Go design and style

- Follow the engineering principles above first. Follow the existing
  repository conventions only when they do not conflict with those principles.
- Constructors set up runtime domain objects and their dependencies. YAML
  models can expose fields for decoding; do not add an empty constructor that
  only wraps a struct literal. Refactor existing globals only inside the
  requested scope.
- Keep methods focused, about 30 lines or fewer. Split them by real domain
  operations, not just to reach a line count. Put shared state on the struct
  that owns it instead of passing it from method to method. Keep values that
  are used once as local variables. A method should use the state its receiver
  owns.

## Tests and data validation

- The `dacfy test` command checks data that already exists in ClickHouse. It
  must not create tables, fixtures, or other data to run a manifest. Go unit
  tests can still use isolated mocks or in-memory fixtures.
- Keep the output of the test command short: the test name, the identifier or
  rule, and PASS or FAIL with a short detail.
- Report expected-row results in manifest order.
- Treat `NULL` differently from the literal string `"<null>"`.
- When you add or extend a test mode, validate the YAML shape before you
  generate SQL, and add focused unit tests for valid behavior and for the
  relevant failures.
- Apply `--suffix` to the test table name without changing the parsed
  manifest.
- Test behavior that users can see and the relevant failures. For a bug fix,
  add a regression test when you can. Avoid tests that only repeat the
  implementation details.

## Documentation

- Put detailed command documentation in `docs/`. Keep `README.md` as the entry
  point, with links to those documents.
- Update the documentation and the examples when a YAML field, a flag, the
  output, or a behavior that users can see changes.
