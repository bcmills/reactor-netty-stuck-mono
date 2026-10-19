import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class CorePublishOnSchedulerWithThreadHandler {
  public static void main(String[] args) throws InterruptedException {
    AtomicReference<Throwable> threadError = new AtomicReference<>();
    ThreadFactory threadFactory = task -> {
      Thread thread = new Thread(task, "core-publish-on-thread-handler");
      thread.setUncaughtExceptionHandler((worker, error) -> {
        threadError.set(error);
        System.err.println("Thread uncaught-exception handler received an error on thread \""
            + worker.getName() + "\":");
        error.printStackTrace(System.err);
      });
      return thread;
    };

    Scheduler scheduler = Schedulers.newSingle(threadFactory);

    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();

      Mono.<String>create(sink -> sink.success("ok"))
          .publishOn(scheduler)
          .map(value -> {
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

      System.out.println("core-publish-on-thread-handler signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " threadHandler=" + (threadError.get() != null));

      if (!"none".equals(signal.get()) || finallySignal.get() != null
          || !(threadError.get() instanceof NoSuchMethodError)) {
        throw new AssertionError("Expected the thread handler to receive NoSuchMethodError"
            + " without calling subscriber callbacks or doFinally");
      }
    } finally {
      scheduler.dispose();
    }
  }
}
