package academy.backend.market_pulse.homework.seminar10;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public final class Dispatcher implements AutoCloseable {
    @FunctionalInterface
    public interface PriceSource {
        BigDecimal fetch(String ticker) throws Exception;
    }

    private final Object monitor = new Object();
    private final Map<String, Subscription> subscriptions = new HashMap<>();
    private final SimpleThreadPool pool;
    private final Thread thread;
    private final PriceSource source;
    private final Consumer<String> output;
    private boolean closed;

    public Dispatcher(int workers, PriceSource source, Consumer<String> output) {
        this.source = Objects.requireNonNull(source);
        this.output = Objects.requireNonNull(output);
        pool = new SimpleThreadPool(workers, error -> System.getLogger(Dispatcher.class.getName())
                .log(System.Logger.Level.ERROR, "Ошибка задачи", error));
        thread = new Thread(this::dispatch, "homework-dispatcher");
        thread.start();
    }

    public void watch(String ticker, Duration interval, Duration lifetime) {
        Subscription subscription = new Subscription(ticker, interval, lifetime);
        synchronized (monitor) {
            if (closed) {
                throw new IllegalStateException("Диспетчер закрыт");
            }
            Subscription previous = subscriptions.get(subscription.ticker);
            if (previous != null && !previous.expired(System.nanoTime())) {
                throw new IllegalArgumentException("Подписка уже существует: " + subscription.ticker);
            }
            if (previous != null) {
                previous.cancel();
            }
            subscriptions.put(subscription.ticker, subscription);
            monitor.notifyAll();
        }
    }

    public boolean remove(String ticker) {
        synchronized (monitor) {
            Subscription subscription = subscriptions.remove(Subscription.normalize(ticker));
            if (subscription == null) {
                return false;
            }
            subscription.cancel();
            monitor.notifyAll();
            return true;
        }
    }

    private void dispatch() {
        synchronized (monitor) {
            while (!closed) {
                long now = System.nanoTime();
                long waitNanos = Duration.ofSeconds(1).toNanos();
                var iterator = subscriptions.values().iterator();
                while (iterator.hasNext()) {
                    Subscription subscription = iterator.next();
                    if (subscription.expired(now)) {
                        subscription.cancel();
                        iterator.remove();
                        continue;
                    }
                    waitNanos = Math.min(waitNanos, subscription.expiresAt - now);
                    if (subscription.state == Subscription.State.WAITING) {
                        if (now - subscription.nextRun >= 0) {
                            subscription.state = Subscription.State.QUEUED;
                            pool.submit(() -> execute(subscription));
                        } else {
                            waitNanos = Math.min(waitNanos, subscription.nextRun - now);
                        }
                    }
                }
                try {
                    waitNanos = Math.max(1, waitNanos);
                    monitor.wait(waitNanos / 1_000_000, (int) (waitNanos % 1_000_000));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void execute(Subscription subscription) {
        synchronized (monitor) {
            if (closed || subscription.state != Subscription.State.QUEUED
                    || subscription.expired(System.nanoTime())) {
                return;
            }
            subscription.state = Subscription.State.RUNNING;
            subscription.runner = Thread.currentThread();
        }
        try {
            String message;
            try {
                BigDecimal price = source.fetch(subscription.ticker);
                message = subscription.ticker + ": " + price.toPlainString();
            } catch (InterruptedException cancelled) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception failure) {
                message = subscription.ticker + ": ошибка — " + failure.getMessage();
            }
            synchronized (monitor) {
                if (!closed && subscription.state != Subscription.State.CANCELLED
                        && !subscription.expired(System.nanoTime()) && !Thread.currentThread().isInterrupted()) {
                    output.accept(message);
                }
            }
        } finally {
            synchronized (monitor) {
                subscription.runner = null;
                if (subscription.state != Subscription.State.CANCELLED) {
                    subscription.nextRun = System.nanoTime() + subscription.intervalNanos;
                    subscription.state = Subscription.State.WAITING;
                }
                monitor.notifyAll();
            }
        }
    }

    @Override
    public void close() {
        synchronized (monitor) {
            if (subscriptions.values().stream().anyMatch(s -> s.runner == Thread.currentThread())) {
                throw new IllegalStateException("Закрывать диспетчер нужно из основного потока");
            }
            closed = true;
            subscriptions.values().forEach(Subscription::cancel);
            subscriptions.clear();
            monitor.notifyAll();
        }
        boolean interrupted = false;
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException error) {
                interrupted = true;
            }
        }
        pool.close();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
