## HEAD

## [reflow0.5.2](https://github.com/grailbio/reflow/releases/tag/reflow0.5.2)

- reflowlet: terminate on 10-minute idle time (since AWS does secondly billing)
- syntax: fix digest for ExprApply
- syntax: {files,dirs}.Copy: return a helpful error message if URL scheme is missing
- runner: improve stealer allocation
- eval: scramble digest when an OpVal carries an error
- syntax,types: fix type environment for recursive module instantiations
- `reflow shell`: new command to shell into an exec
- tool: use stdout for reflow doc output
- tool: disable -gc for v1 flows, -dir alias for -localdir, for backwards compatibility
- `reflow logs -f`: support for tailing ongoing exec logs
- tool: restore noexterncache flag
- syntax: always sort map keys carefully before digesting
- syntax: fix environment persistence for declarations
- doc: add language overview docs
- config: add "local" configuration provider for user
- eval: print full image name in debug logs
- syntax: add few missing cases for trailing commas
- syntax: add a semicolon at the end of a file if there is a missing newline
- syntax: add arbitrary precision float, conversion between int and float
- eval: add option/flag -recomputeempty to recompute empty cache values

## [reflow0.5.1](https://github.com/grailbio/reflow/releases/tag/reflow0.5.1)

Bugfix release to include proper library vendoring, causing intermittent Docker issues.

- Fix `vendor/` directory to include the correct Docker dependencies.
- tool: create the config directory if it does not exist

## [reflow0.5](https://github.com/grailbio/reflow/releases/tag/reflow0.5)

Initial public release of Reflow.
