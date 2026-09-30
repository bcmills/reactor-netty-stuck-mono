# Reactor Netty Mono without a terminal signal or uncaught exception

A loopback server returns `ok`. The Reactor Netty HTTP client receives it, and
a response-body callback deliberately throws a simulated `NoSuchMethodError`.
Netty logs the failure, but the resulting Mono's subscriber sees no value,
`onError`, or `onComplete`, and the JVM's default uncaught exception handler
does not run. This example has no retry operator, Spring, or real dependency
incompatibility.

## Run

Requires JDK 17+ and internet access on the first run for the Gradle wrapper
and public Maven Central dependencies:

```sh
./gradlew --no-daemon -q run
```

The expected summary, after Netty's `exceptionCaught()` warning, is:

```text
signal=none uncaught=false
```

The warning and summary were reproduced with the versions listed below on
JDK 21/macOS; the Netty transport resolved to 4.2.18.Final.

The program checks that the server received a request and fails if the Mono
produces a signal or an uncaught exception. The two-second wait demonstrates
the absence of a signal *during that interval*, not a proof of an infinite hang.
`src/main/resources/simplelogger.properties` keeps Netty's warning visible but
silences duplicate Reactor logs; remove it to inspect all logs.

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
