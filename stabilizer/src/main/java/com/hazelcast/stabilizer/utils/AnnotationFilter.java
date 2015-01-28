package com.hazelcast.stabilizer.utils;

import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;

import java.lang.annotation.Annotation;

/**
 * This class filters Annotations, e.g. after their values.
 *
 * @param <A>    Class of type Annotation
 */
public interface AnnotationFilter<A extends Annotation> {
    boolean allowed(A annotation);

    public static class AlwaysFilter implements AnnotationFilter<Annotation> {
        @Override
        public boolean allowed(Annotation annotation) {
            return true;
        }
    }

    public class TeardownFilter implements AnnotationFilter<Teardown> {
        private final boolean isGlobal;

        public TeardownFilter(boolean isGlobal) {
            this.isGlobal = isGlobal;
        }

        @Override
        public boolean allowed(Teardown teardown) {
            return teardown.global() == isGlobal;
        }
    }

    public class WarmupFilter implements AnnotationFilter<Warmup> {
        private final boolean isGlobal;

        public WarmupFilter(boolean isGlobal) {
            this.isGlobal = isGlobal;
        }

        @Override
        public boolean allowed(Warmup verify) {
            return verify.global() == isGlobal;
        }
    }

    public class VerifyFilter implements AnnotationFilter<Verify> {
        private final boolean isGlobal;

        public VerifyFilter(boolean isGlobal) {
            this.isGlobal = isGlobal;
        }

        @Override
        public boolean allowed(Verify verify) {
            return verify.global() == isGlobal;
        }
    }
}
