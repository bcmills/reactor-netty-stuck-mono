import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class CorePublishOnSchedulerErrorHook {
  public static void main(String[] args) throws InterruptedException {
    AtomicReference<Throwable> handledError = new AtomicReference<>();
    String hookKey = "core-publish-on-scheduler-error-hook";
    Scheduler scheduler = Schedulers.newSingle("core-publish-on-error-hook");
    Schedulers.onHandleError(hookKey, (thread, error) -> {
      handledError.set(error);
      System.err.println("Scheduler error hook on thread \"" + thread.getName() + "\":");
      error.printStackTrace(System.err);
    });
    try {
      AtomicReference<String> signal = new AtomicReference<>("none");
      AtomicReference<SignalType> finallySignal = new AtomicReference<>();
      AtomicReference<NoSuchMethodError> failure = new AtomicReference<>();
      AtomicReference<MonoSink<String>> sourceSink = new AtomicReference<>();
      Mono.<String>create(sourceSink::set)
          .publishOn(scheduler)
          .map(value -> {
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

      Thread producer = new Thread(() -> sourceSink.get().success("ok"), "core-mono-producer");
      producer.start();
      producer.join(5000);
      if (producer.isAlive()) {
        throw new AssertionError("The control's producer thread did not terminate");
      }
      // Wait for emission, the scheduler's own handler, and the error hook to finish.
      CountDownLatch schedulerDrained = new CountDownLatch(1);
      scheduler.schedule(schedulerDrained::countDown);
      if (!schedulerDrained.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("The control's scheduler did not finish processing the emission");
      }
      System.out.println("core-publish-on-error-hook signal=" + signal.get() + " doFinally="
          + (finallySignal.get() == null ? "none" : finallySignal.get())
          + " errorHook=" + (handledError.get() != null));
      if (!"none".equals(signal.get()) || finallySignal.get() != null
          || failure.get() == null || handledError.get() != failure.get()) {
        throw new AssertionError("Expected the original fatal error in the scheduler error hook without signals");
      }
    } finally {
      scheduler.dispose();
      Schedulers.resetOnHandleError(hookKey);
    }
  }
}
