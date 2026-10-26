package dev.rakshit.kv;

/**
 * Fault-injection points. The store calls {@link #at} just before and after each step whose interruption
 * could corrupt state (see {@link KvStore}). Production code uses {@link #NONE}; tests supply an
 * implementation that throws {@link SimulatedCrash} at a chosen point, then reopen the directory to prove
 * recovery works from exactly that on-disk state.
 */
@FunctionalInterface
interface Hooks {
    Hooks NONE = point -> {};

    void at(String point);

    /** Thrown by test hooks to model the process dying at a point. Nothing after it runs. */
    final class SimulatedCrash extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SimulatedCrash(String point) {
            super("simulated crash at " + point);
        }
    }
}
