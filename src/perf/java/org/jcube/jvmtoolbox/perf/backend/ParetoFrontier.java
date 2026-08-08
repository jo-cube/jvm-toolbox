package org.jcube.jvmtoolbox.perf.backend;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.ToDoubleFunction;

public final class ParetoFrontier {
    private ParetoFrontier() {}

    public static <T> List<T> efficient(List<T> candidates, List<Objective<T>> objectives) {
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(objectives, "objectives");
        if (objectives.isEmpty()) {
            throw new IllegalArgumentException("at least one Pareto objective is required");
        }
        var frontier = new ArrayList<T>();
        for (T candidate : candidates) {
            boolean dominated = false;
            for (T other : candidates) {
                if (candidate != other && dominates(other, candidate, objectives)) {
                    dominated = true;
                    break;
                }
            }
            if (!dominated) {
                frontier.add(candidate);
            }
        }
        return List.copyOf(frontier);
    }

    private static <T> boolean dominates(T left, T right, List<Objective<T>> objectives) {
        boolean better = false;
        for (Objective<T> objective : objectives) {
            double leftValue = objective.value().applyAsDouble(left);
            double rightValue = objective.value().applyAsDouble(right);
            int comparison = objective.tolerance().equivalent(leftValue, rightValue)
                    ? 0
                    : Double.compare(leftValue, rightValue) * objective.direction().sign;
            if (comparison < 0) {
                return false;
            }
            better |= comparison > 0;
        }
        return better;
    }

    public record Objective<T>(
            Direction direction, ToDoubleFunction<T> value, Tolerance tolerance) {
        public Objective {
            Objects.requireNonNull(direction, "direction");
            Objects.requireNonNull(value, "value");
            Objects.requireNonNull(tolerance, "tolerance");
        }

        public static <T> Objective<T> maximize(ToDoubleFunction<T> value) {
            return maximize(value, Tolerance.exact());
        }

        public static <T> Objective<T> maximize(
                ToDoubleFunction<T> value, Tolerance tolerance) {
            return new Objective<>(Direction.MAXIMIZE, value, tolerance);
        }

        public static <T> Objective<T> minimize(ToDoubleFunction<T> value) {
            return minimize(value, Tolerance.exact());
        }

        public static <T> Objective<T> minimize(
                ToDoubleFunction<T> value, Tolerance tolerance) {
            return new Objective<>(Direction.MINIMIZE, value, tolerance);
        }
    }

    public record Tolerance(double absolute, double relative) {
        public Tolerance {
            if (!Double.isFinite(absolute)
                    || !Double.isFinite(relative)
                    || absolute < 0
                    || relative < 0) {
                throw new IllegalArgumentException("Pareto tolerances must be finite and non-negative");
            }
        }

        public static Tolerance exact() {
            return new Tolerance(0, 0);
        }

        public static Tolerance absolute(double tolerance) {
            return new Tolerance(tolerance, 0);
        }

        public static Tolerance relative(double tolerance) {
            return new Tolerance(0, tolerance);
        }

        public static Tolerance of(double absolute, double relative) {
            return new Tolerance(absolute, relative);
        }

        boolean equivalent(double left, double right) {
            if (!Double.isFinite(left) || !Double.isFinite(right)) {
                throw new IllegalArgumentException("Pareto objective values must be finite");
            }
            if (Double.compare(left, right) == 0) {
                return true;
            }
            double allowed = Math.max(absolute, relative * Math.max(Math.abs(left), Math.abs(right)));
            return Math.abs(left - right) <= allowed;
        }
    }

    public enum Direction {
        MAXIMIZE(1),
        MINIMIZE(-1);

        private final int sign;

        Direction(int sign) {
            this.sign = sign;
        }
    }
}
