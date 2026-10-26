package dev.rakshit.kv.testkit;

import java.util.Objects;

public final class Assert {
    private Assert() {}

    public static void assertTrue(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void assertTrue(boolean condition) {
        assertTrue(condition, "expected true");
    }

    public static void assertFalse(boolean condition, String message) {
        if (condition) throw new AssertionError(message);
    }

    public static void assertFalse(boolean condition) {
        assertFalse(condition, "expected false");
    }

    public static void assertEquals(Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError("expected <" + expected + "> but was <" + actual + ">");
        }
    }

    public static void assertNull(Object actual) {
        if (actual != null) throw new AssertionError("expected null but was <" + actual + ">");
    }

    public static void assertInRange(double actual, double lo, double hi, String what) {
        if (actual < lo || actual > hi) {
            throw new AssertionError(what + " = " + actual + ", expected within [" + lo + ", " + hi + "]");
        }
    }

    /** Like Runnable, but the body may throw checked exceptions. */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** Runs the body and fails unless it throws {@code type}. */
    public static void assertThrows(Class<? extends Throwable> type, ThrowingRunnable body) {
        try {
            body.run();
        } catch (Throwable t) {
            if (type.isInstance(t)) return;
            throw new AssertionError("expected " + type.getSimpleName() + " but got " + t);
        }
        throw new AssertionError("expected " + type.getSimpleName() + " but nothing was thrown");
    }
}
