import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class CorePublishOnSchedulerDefault {
  public static void main(String[] args) throws InterruptedException {
    Thread.UncaughtExceptionHandler previousHandler = Thread.getDefaultUncaughtExceptionHandler();
    AtomicReference<Throwable> uncaught = new AtomicReference<>();
    Thread.setDefaultUncaughtExceptionHandler((thread, error) -> {
      uncaught.set(error);
      System.err.println("Exception in thread \"" + thread.getName() + "\"");
      error.printStackTrace(System.err);
    });
    Scheduler scheduler = Schedulers.newSingle("core-publish-on-control");
    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      AtomicReference<NoSuchMethodError> failure = new AtomicReference<>();
      AtomicReference<MonoSink<String>> sourceSink = new AtomicReference<>();
      Mono.<String>create(sourceSink::set)
          .publishOn(scheduler)
          .map(value -> {
            // Capture the scheduler's active operator/emission call path in the stack trace.
            NoSuchMethodError error = new NoSuchMethodError("simulated incompatible dependency");
            failure.set(error);
            throw error;
          })
          .doFinally(finallySignal::set)
          .subscribe(value -> signal.set("value"),
              error -> {
                signal.set("onError");
                System.err.println("Mono subscriber received onError:");
                error.printStackTrace(System.err);
              },
              () -> signal.set("onComplete"));

      // Start emission after subscribe returns, so the failure cannot escape that call.
      Thread producer = new Thread(() -> sourceSink.get().success("ok"), "core-mono-producer");
      producer.start();
      producer.join(5000);
      if (producer.isAlive()) {
        throw new AssertionError("The control's producer thread did not terminate");
      }

      // A later task on this single-thread scheduler runs after emission and error handling.
      CountDownLatch schedulerDrained = new CountDownLatch(1);
      scheduler.schedule(schedulerDrained::countDown);
      if (!schedulerDrained.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The control's scheduler did not finish processing the emission");
      }
      if (failure.get() == null) {
        throw new AssertionError("The failing map callback did not run");
      }
      System.out.println("core-publish-on signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " uncaught=" + (uncaught.get() != null));
      if (!"none".equals(signal.get()) || finallySignal.get() != null || uncaught.get() != null) {
        throw new AssertionError("Expected no subscriber signal, doFinally, or default uncaught-handler call");
      }
    } finally {
      scheduler.dispose();
      Thread.setDefaultUncaughtExceptionHandler(previousHandler);
    }
  }
}
