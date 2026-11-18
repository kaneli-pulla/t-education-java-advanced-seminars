package academy.backend.market_pulse.homework.seminar10;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

final class Subscription {
    enum State { WAITING, QUEUED, RUNNING, CANCELLED }

    final String ticker;
    final long intervalNanos;
    final long expiresAt;
    long nextRun;
    State state = State.WAITING;
    Thread runner;

    Subscription(String ticker, Duration interval, Duration lifetime) {
        this.ticker = normalize(ticker);
        intervalNanos = positiveNanos(interval);
        long lifetimeNanos = positiveNanos(lifetime);
        nextRun = System.nanoTime();
        expiresAt = nextRun + lifetimeNanos;
    }

    static String normalize(String ticker) {
        String value = Objects.requireNonNull(ticker).trim().toUpperCase(Locale.ROOT);
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Тикер не должен быть пустым");
        }
        return value;
    }

    private static long positiveNanos(Duration duration) {
        Objects.requireNonNull(duration);
        try {
            long nanos = duration.toNanos();
            if (nanos <= 0 || nanos > Duration.ofDays(365).toNanos()) {
                throw new IllegalArgumentException("Интервал и срок должны быть от 1 нс до 365 дней");
            }
            return nanos;
        } catch (ArithmeticException error) {
            throw new IllegalArgumentException("Слишком большая длительность", error);
        }
    }

    boolean expired(long now) {
        return now - expiresAt >= 0;
    }

    void cancel() {
        state = State.CANCELLED;
        if (runner != null) {
            runner.interrupt();
        }
    }
}
