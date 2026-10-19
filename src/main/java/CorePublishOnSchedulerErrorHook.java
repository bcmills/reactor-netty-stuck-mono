import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class CorePublishOnSchedulerErrorHook {
  public static void main(String[] args) throws InterruptedException {
    AtomicReference<Throwable> handledError = new AtomicReference<>();
    String hookKey = "core-publish-on-scheduler-error-hook";
    Schedulers.onHandleError(hookKey, (thread, error) -> {
      handledError.set(error);
      System.err.println("Scheduler error hook received an error on thread \"" + thread.getName() + "\":");
      error.printStackTrace(System.err);
    });

    Scheduler scheduler = Schedulers.newSingle("core-publish-on-error-hook");

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
      // Wait for it so the checks below cannot run before the error hook finishes.
      CountDownLatch schedulerDrained = new CountDownLatch(1);
      scheduler.schedule(schedulerDrained::countDown);
      if (!schedulerDrained.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The scheduler did not run the task queued after the publisher");
      }

      System.out.println("core-publish-on-error-hook signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " errorHook=" + (handledError.get() != null));

      if (!"none".equals(signal.get()) || finallySignal.get() != null
          || !(handledError.get() instanceof NoSuchMethodError)) {
        throw new AssertionError("Expected the scheduler error hook to receive NoSuchMethodError"
            + " without calling subscriber callbacks or doFinally");
      }
    } finally {
      scheduler.dispose();
      Schedulers.resetOnHandleError(hookKey);
    }
  }
}
