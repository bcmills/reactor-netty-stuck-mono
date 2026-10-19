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
./gradlew --no-daemon -q runNettyEventLoop
```

Each Java class names the setup it checks, and its Gradle task is `run<ClassName>`.
`SchedulerDefault` uses Core's named scheduler factory; `SchedulerWithThread`
supplies a factory that creates ordinary Java threads. The standard `run` task
also runs `NettyEventLoop`.

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
on stderr, including their stack traces. The examples' default uncaught-exception
handlers and scheduler error hook also print the thread name and full stack
trace to stderr. Summaries remain on stdout.

## Reactor Core controls

Run the non-Netty controls with the same Reactor Core version:

```sh
./gradlew --no-daemon -q runCoreControls
```

Each control has its own source file and runnable task. All apply the same
throwing `map` callback, subscriber callbacks, and `doFinally` observation.
They observe errors through the default uncaught-exception handler or a
scheduler error hook:

- **Synchronous** (`src/main/java/CoreSyncSubscribe.java`): `Mono.just("ok")`
  emits during `subscribe`. The original
  `NoSuchMethodError` escapes to the caller, which catches it for the assertion.
  The catch prints `Mono.subscribe threw to its caller:` and the stack trace to
  stderr. The control constructs the error inside `map` so its trace includes
  the active operator and `subscribe` call path. The trace records where the
  error was constructed; rethrows do not add new frames. The subscriber and
  `doFinally` receive no signal.
- **Asynchronous** (`src/main/java/CoreCreateWithThread.java`): `Mono.create`
  emits `"ok"` from a plain Java thread through
  `MonoSink.success`. The subscriber receives the original `NoSuchMethodError`
  through `onError`, and `doFinally` runs with `onError`. The control joins the
  producer thread before checking the results. It constructs the error inside
  `map` so the trace includes the producer's operator and `MonoSink.success`
  call path. The error callback prints `Mono subscriber received onError:` and
  the stack trace to stderr.
- **With `publishOn` and Core's logging handler**
  (`src/main/java/CorePublishOnSchedulerDefault.java`):
  `Mono.create` emits from a plain Java thread after `subscribe` returns, and
  `publishOn(Schedulers.newSingle(...))` moves the failing `map` to a scheduler
  thread. The scheduler logs the fatal error, but neither the subscriber nor
  `doFinally` receives a signal. This tests the
  [maintainer's scenario](https://github.com/reactor/reactor-netty/issues/4407#issuecomment-6058013320).
  After joining the producer, the control queues a marker task on the same
  single-thread scheduler and waits for it. That establishes that the failing
  emission and scheduler error handling have finished before checking signals.
- **With `publishOn` and ordinary Java threads**
  (`src/main/java/CorePublishOnSchedulerWithThread.java`): the same scheduled
  failure reaches the JVM default uncaught-exception handler. This control
  supplies a `ThreadFactory` instead of using Core's named scheduler factory.
  Neither the subscriber nor `doFinally` receives a signal; the control checks
  that the handler received the original error object.
- **With `publishOn` and a scheduler error hook**
  (`src/main/java/CorePublishOnSchedulerErrorHook.java`): Core's named scheduler
  retains its usual logging handler, while a keyed `Schedulers.onHandleError`
  hook receives the original fatal error and prints its stack trace. This case
  does not set a JVM default uncaught-exception handler or supply a thread
  factory. The hook is removed by key in `finally`.

To run a control separately:

```sh
./gradlew --no-daemon -q runCoreSyncSubscribe
./gradlew --no-daemon -q runCoreCreateWithThread
./gradlew --no-daemon -q runCorePublishOnSchedulerDefault
./gradlew --no-daemon -q runCorePublishOnSchedulerWithThread
./gradlew --no-daemon -q runCorePublishOnSchedulerErrorHook
```

Expected stdout summaries (warning and error logs go to stderr):

```text
core-sync signal=none doFinally=none uncaught=false thrown=true
core-async signal=onError doFinally=onError uncaught=false
core-publish-on signal=none doFinally=none uncaught=false
core-publish-on-handler signal=none doFinally=none uncaught=true
core-publish-on-error-hook signal=none doFinally=none errorHook=true
```

The synchronous and direct-emission asynchronous cases assert the error's
identity, so wrapping or substituting an error fails those controls. The
`publishOn` control checks that the failing `map` actually ran and that no
subscriber signal, `doFinally`, or default uncaught-handler call followed.
The control code uses only Reactor Core and Java APIs.

These cases distinguish the lack of a terminal signal from the loss of the
Java throw. The synchronous control shares the HTTP example's lack of signals,
but its caller receives the fatal error. The asynchronous control terminates
with an error signal. Adding `publishOn` reproduces the HTTP example's observable
combination of no signal and no default uncaught-handler call. The lack of
terminal signals is therefore also reproducible with Reactor Core alone.

The result depends on the source's execution boundary. In Reactor Core 3.8.7,
[`MonoSink.success` catches a downstream throw and calls `onError`](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/publisher/MonoCreate.java#L174-L185),
even though the `map` operator treats this error as fatal and rethrows it.

`uncaught` measures calls to the **JVM default** uncaught-exception handler,
not every thread-specific handler. Core's `newSingle` scheduler
[installs its own handler](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/scheduler/ReactorThreadFactory.java#L56-L77).
[`SchedulerTask` catches the throw](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/scheduler/SchedulerTask.java#L71-L76)
and [`Schedulers.handleError` invokes the thread's handler, which logs the error](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/scheduler/Schedulers.java#L1277-L1288).
Thus `uncaught=false` in this control does **not** mean no uncaught-exception
handler ran. It also does not mean the scheduler thread died: the later marker
task demonstrates that it continued processing work. These controls do not
establish how every Core source or scheduler handles fatal errors.

## Error boundaries and caller-controlled reporting

The original HTTP reproducer uses **Netty's channel pipeline**, not Reactor's
`SchedulerTask`, as the boundary around the throwing callback. Reactor Netty
[`ChannelOperationsHandler.channelRead` catches the throw, logs it, closes the
channel, and invokes `exceptionCaught`](https://github.com/reactor/reactor-netty/blob/v1.3.7/reactor-netty-core/src/main/java/reactor/netty/channel/ChannelOperationsHandler.java#L138-L158).
Netty's [`fireChannelRead` catches the resulting throw](https://github.com/netty/netty/blob/netty-4.2.18.Final/transport/src/main/java/io/netty/channel/AbstractChannelHandlerContext.java#L341-L360)
and [`invokeExceptionCaught` catches and logs a further throw from that handler](https://github.com/netty/netty/blob/netty-4.2.18.Final/transport/src/main/java/io/netty/channel/AbstractChannelHandlerContext.java#L287-L303).
The original runtime trace passes through `channelRead` and Netty's I/O loop;
the Core `publishOn` trace instead passes through `MonoPublishOn.run` and
`SchedulerTask.call`. The two cases have analogous asynchronous boundaries
and share Core's fatal-error rethrow behavior, but the final catches differ.

Core exposes a way to control its scheduler's handler delivery:

```java
Scheduler scheduler = Schedulers.newSingle(task -> new Thread(task, "caller-worker"));
```

[`newSingle(ThreadFactory)` uses the caller's factory](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/scheduler/Schedulers.java#L585-L597).
With no explicit thread handler, the ordinary thread's `ThreadGroup` delegates
to the JVM default uncaught-exception handler. `Schedulers.handleError` invokes
that handler with the original fatal error. This is **handler notification**:
the scheduler still catches the throw, the worker continues, and the Mono still
does not terminate. It does not rethrow on the subscribing thread or inherently
terminate the worker or process.

For application-level scheduler error reporting, Reactor also provides
[`Schedulers.onHandleError(key, hook)`](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/scheduler/Schedulers.java#L626-L650).
The error-hook sample uses this purpose-built API instead of changing thread
construction or the JVM default handler. The hook is global across Reactor
schedulers; its key controls registration and removal, not which scheduler's
errors it observes. Removing only the owned key preserves other registered
hooks. In Core 3.8.7,
[`Schedulers.handleError` invokes the hook after the thread's handler](https://github.com/reactor/reactor-core/blob/v3.8.7/reactor-core/src/main/java/reactor/core/scheduler/Schedulers.java#L1282-L1295);
this sample verifies that the original `NoSuchMethodError` reaches it, despite
the API Javadoc's fatal-error caveat. Core's normal error log still appears.
Like handler delivery, the hook does not
terminate the Mono or worker, and it does not observe failures caught solely
inside Netty's channel pipeline. A throwing HTTP callback moved onto a Reactor
scheduler with `publishOn` goes through the same scheduler error-handling path.

The custom-thread-factory technique also works for the throwing HTTP callback
in this repo:

```sh
./gradlew --no-daemon -q runNettyPublishOnSchedulerWithThread
```

`src/main/java/NettyPublishOnSchedulerWithThread.java` inserts `publishOn(scheduler)`
inside `responseSingle`, between `body.asString()` and the throwing `map`.
The resulting summary is:

```text
netty-publish-on-handler signal=none doFinally=none uncaught=true
```

The fatal-error trace now contains `MonoPublishOn.run` and `SchedulerTask.call`,
and the default handler receives the original error. Placing `publishOn` only
after `responseSingle` would be too late for a failure inside its body callback.
This technique does not cover failures upstream of the boundary, such as an
exception in a Netty HTTP/Brotli decoder. The original production decoder
failure is not reproduced here, so this is not evidence of a fix for it.

If a terminal `onError` signal is required instead, the failing function can
explicitly catch `NoSuchMethodError` and wrap it in a non-fatal exception before
Core sees it. That changes the fatal-error policy and is separate from handler
delivery; the handler controls preserve the original fatal error unchanged.

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
