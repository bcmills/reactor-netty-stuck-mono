# Reactor Netty Mono without a terminal signal or uncaught exception

A loopback server returns `ok`. The Reactor Netty HTTP client receives it, and
a response-body callback deliberately throws a simulated `NoSuchMethodError`.
Netty logs the failure, but the resulting Mono's subscriber sees no value,
`onError`, or `onComplete`, and the JVM's default uncaught exception handler
does not run. This example has no retry operator, Spring, or real dependency
incompatibility.

Upstream issue: [reactor/reactor-netty#4407](https://github.com/reactor/reactor-netty/issues/4407).

## Run

Requires JDK 17+ and internet access on the first run for the Gradle wrapper
and public Maven Central dependencies:

```sh
./gradlew --no-daemon -q run
```

The expected summary, after Reactor and Netty's warning and error logs, is:

```text
signal=none doFinally=none uncaught=false
```

The warning and summary were reproduced with the versions listed below on
JDK 21/macOS; the Netty transport resolved to 4.2.18.Final.

The program checks that the server received a request and fails if the Mono
produces a signal or an uncaught exception. The two-second wait demonstrates
the absence of a signal *during that interval*, not a proof of an infinite hang.
`doFinally` is attached after the HTTP Mono to observe whether its subscription
terminates or is cancelled, even if the subscriber receives no terminal signal.
`src/main/resources/simplelogger.properties` enables all warning and error logs
on stderr, including their stack traces. Each example's default uncaught-exception
handler also prints the thread name and full stack trace to stderr. Summaries
remain on stdout.

## Reactor Core controls

Run the non-Netty controls with the same Reactor Core version:

```sh
./gradlew --no-daemon -q runCoreControl
```

Each control has its own source file and runnable task. Both apply the same
throwing `map` callback, subscriber callbacks, `doFinally` observation, and
default uncaught-exception handler:

- **Synchronous** (`src/main/java/SyncCoreMonoControl.java`): `Mono.just("ok")`
  emits during `subscribe`. The original
  `NoSuchMethodError` escapes to the caller, which catches it for the assertion.
  The catch prints `Mono.subscribe threw to its caller:` and the stack trace to
  stderr. The control constructs the error inside `map` so its trace includes
  the active operator and `subscribe` call path. The trace records where the
  error was constructed; rethrows do not add new frames. The subscriber and
  `doFinally` receive no signal.
- **Asynchronous** (`src/main/java/AsyncCoreMonoControl.java`): `Mono.create`
  emits `"ok"` from a plain Java thread through
  `MonoSink.success`. The subscriber receives the original `NoSuchMethodError`
  through `onError`, and `doFinally` runs with `onError`. The control joins the
  producer thread before checking the results. It constructs the error inside
  `map` so the trace includes the producer's operator and `MonoSink.success`
  call path. The error callback prints `Mono subscriber received onError:` and
  the stack trace to stderr.

To run either control separately:

```sh
./gradlew --no-daemon -q runCoreSyncControl
./gradlew --no-daemon -q runCoreAsyncControl
```

Expected stdout summaries (warning and error logs go to stderr):

```text
core-sync signal=none doFinally=none uncaught=false thrown=true
core-async signal=onError doFinally=onError uncaught=false
```

Both cases assert the error's identity, so wrapping or substituting an error
fails the control. The control code uses only Reactor Core and Java APIs.

These cases distinguish the lack of a terminal signal from the loss of the
Java throw. The synchronous control shares the HTTP example's lack of signals,
but its caller receives the fatal error. The asynchronous control terminates
with an error signal. Neither has the HTTP example's combination of no signal
and no escaped error.

The result depends on the source's execution boundary. In Reactor Core 3.8.7,
[`MonoSink.success` catches a downstream throw and calls `onError`](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/publisher/MonoCreate.java#L174-L185),
even though the `map` operator treats this error as fatal and rethrows it.
These controls do not establish how every Core source or scheduler handles
fatal errors.

## Mechanism and scope

The throw occurs in a response-body `map` callback on Netty's event loop.
Reactor treats `LinkageError` as fatal and rethrows it; Reactor Netty invokes
its `exceptionCaught` handler, whose throw is caught and logged by Netty.
The example does **not** demonstrate the original Brotli decoder failure or
the downstream retry/memoizer behavior. As checked September 30, 2026, it
uses the latest stable [Gradle 9.8.0](https://services.gradle.org/versions/current),
[Reactor Netty 1.3.7](https://github.com/reactor/reactor-netty/releases/tag/v1.3.7),
[Reactor Core 3.8.7](https://github.com/reactor/reactor-core/releases/tag/v3.8.7),
[Netty 4.2.18.Final](https://github.com/netty/netty/releases/tag/netty-4.2.18.Final),
and [SLF4J 2.0.20](https://github.com/qos-ch/slf4j/releases/tag/v_2.0.20).
Reactor Netty 1.3.7 declares Netty 4.2.17.Final; the Netty BOM selects its
newer 4.2.18.Final patch release for this reproduction.
