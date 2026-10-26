package dev.rakshit.kv.testkit;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Runs every {@link Test} method of the classes named on the command line, each on a fresh instance.
 * Exit code 0 if all pass, 1 otherwise. Usage: {@code java ... Runner pkg.ClassA pkg.ClassB}
 */
public final class Runner {
    public static void main(String[] classNames) throws Exception {
        int passed = 0;
        List<String> failures = new ArrayList<>();
        for (String name : classNames) {
            Class<?> cls = Class.forName(name);
            Method[] methods = cls.getDeclaredMethods();
            Arrays.sort(methods, Comparator.comparing(Method::getName));
            for (Method m : methods) {
                if (!m.isAnnotationPresent(Test.class)) continue;
                String label = cls.getSimpleName() + "." + m.getName();
                try {
                    m.setAccessible(true);
                    m.invoke(cls.getDeclaredConstructor().newInstance());
                    passed++;
                    System.out.println("  PASS  " + label);
                } catch (InvocationTargetException e) {
                    failures.add(label);
                    System.out.println("  FAIL  " + label + "  ->  " + e.getCause());
                }
            }
        }
        System.out.println();
        System.out.println("passed: " + passed + "   failed: " + failures.size());
        System.exit(failures.isEmpty() ? 0 : 1);
    }
}
