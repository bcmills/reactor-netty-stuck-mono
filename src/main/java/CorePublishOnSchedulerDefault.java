import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class CorePublishOnSchedulerDefault {
  public static void main(String[] args) throws InterruptedException {
    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      uncaught.set(error);
      System.err.println("Default uncaught-exception handler received an error on thread \""
          + thread.getName() + "\":");
      error.printStackTrace(System.err);
    });

    Scheduler scheduler = Schedulers.newSingle("core-publish-on-control");

    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      AtomicBoolean mapCalled = new AtomicBoolean();

      Mono.<String>create(sink -> sink.success("ok"))
          .publishOn(scheduler)
          .map(value -> {
            mapCalled.set(true);
            throw new NoSuchMethodError("simulated dependency lacks the required method");
          })
          .doFinally(finallySignal::set)
          .subscribe(value -> signal.set("value"),
              error -> {
                signal.set("onError");
                System.err.println("Subscriber received an error through onError:");
                error.printStackTrace(System.err);
              },
              () -> signal.set("onComplete"));

      // Queue a task that runs on this scheduler thread after the publisher task finishes.
      // Wait for it so the checks below cannot run before the error handler finishes.
      CountDownLatch schedulerDrained = new CountDownLatch(1);
      scheduler.schedule(schedulerDrained::countDown);
      if (!schedulerDrained.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The scheduler did not run the task queued after the publisher");
      }

      System.out.println("core-publish-on signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null));

      if (!mapCalled.get()) {
        throw new AssertionError("The failing map callback did not run");
      }
      if (!"none".equals(signal.get()) || finallySignal.get() != null || uncaught.get() != null) {
        throw new AssertionError("Expected subscriber callbacks, doFinally,"
            + " and the default uncaught-exception handler not to run");
      }
    } finally {
      scheduler.dispose();
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
