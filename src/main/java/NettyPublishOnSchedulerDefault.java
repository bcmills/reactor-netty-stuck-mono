import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

public final class NettyPublishOnSchedulerDefault {
  public static void main(String[] args) throws InterruptedException {
    DisposableServer server = HttpServer.create()
        .host("127.0.0.1")
        .port(0)
        .handle((request, response) -> response.sendString(Mono.just("ok")))
        .bindNow();

    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      uncaught.set(error);
      System.err.println("Default uncaught-exception handler received an error on thread \""
          + thread.getName() + "\":");
      error.printStackTrace(System.err);
    });

    Scheduler scheduler = Schedulers.newSingle("netty-publish-on-default");

    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      CountDownLatch mapCalled = new CountDownLatch(1);

      HttpClient.create()
          .get()
          .uri("http://127.0.0.1:" + server.port())
          .responseSingle((response, body) -> body.asString()
              // Move this callback off Netty's event loop before it can throw.
              .publishOn(scheduler)
              .map(value -> {
                mapCalled.countDown();
                throw new NoSuchMethodError("simulated dependency lacks the required method");
              }))
          .doFinally(finallySignal::set)
          .subscribe(value -> signal.set("value"),
              error -> {
                signal.set("onError");
                System.err.println("Subscriber received an error through onError:");
                error.printStackTrace(System.err);
              },
              () -> signal.set("onComplete"));

      // HTTP completes asynchronously; wait until map starts before queueing the next task.
      if (!mapCalled.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The failing map callback did not run");
      }

      // Queue a task that runs on this scheduler thread after the publisher task finishes.
      // Wait for it so the checks below cannot run before the error handler finishes.
      CountDownLatch schedulerDrained = new CountDownLatch(1);
      scheduler.schedule(schedulerDrained::countDown);
      if (!schedulerDrained.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The scheduler did not run the task queued after the publisher");
      }

      System.out.println("netty-publish-on-default signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null));

      if (!"none".equals(signal.get()) || finallySignal.get() != null || uncaught.get() != null) {
        throw new AssertionError("Expected subscriber callbacks, doFinally,"
            + " and the default uncaught-exception handler not to run");
      }
    } finally {
      scheduler.dispose();
      server.disposeNow();
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
