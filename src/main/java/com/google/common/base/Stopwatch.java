/*
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.base;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

/**
 * POUR GAGNER 2.5Mb SUR LA TAILLE DU PACKAGE
 */
@SuppressWarnings("GoodTime") // lots of violations
public final class Stopwatch {
	private boolean isRunning;
	private long elapsedNanos;
	private long startTick;

	/**
	 * Creates (and starts) a new stopwatch using {@link System#nanoTime} as its time source.
	 *
	 * @since 15.0
	 */
	public static Stopwatch createStarted() {
		return new Stopwatch().start();
	}

	Stopwatch() {
	}

	/**
	 * Returns {@code true} if {@link #start()} has been called on this stopwatch, and {@link #stop()}
	 * has not been called since the last call to {@code start()}.
	 */
	public boolean isRunning() {
		return isRunning;
	}

	/**
	 * Starts the stopwatch.
	 *
	 * @return this {@code Stopwatch} instance
	 * @throws IllegalStateException if the stopwatch is already running.
	 */
	public Stopwatch start() {
		isRunning = true;
		startTick = System.nanoTime();
		return this;
	}

	/**
	 * Stops the stopwatch. Future reads will return the fixed duration that had elapsed up to this
	 * point.
	 *
	 * @return this {@code Stopwatch} instance
	 * @throws IllegalStateException if the stopwatch is already stopped.
	 */
	public Stopwatch stop() {
		final long tick = System.nanoTime();
		isRunning = false;
		elapsedNanos += tick - startTick;
		return this;
	}

	/**
	 * Sets the elapsed time for this stopwatch to zero, and places it in a stopped state.
	 *
	 * @return this {@code Stopwatch} instance
	 */
	public Stopwatch reset() {
		elapsedNanos = 0;
		isRunning = false;
		return this;
	}

	private long elapsedNanos() {
		return isRunning ? System.nanoTime() - startTick + elapsedNanos : elapsedNanos;
	}

	/**
	 * Returns the current elapsed time shown on this stopwatch, expressed in the desired time unit,
	 * with any fraction rounded down.
	 *
	 * <p><b>Note:</b> the overhead of measurement can be more than a microsecond, so it is generally
	 * not useful to specify {@link TimeUnit#NANOSECONDS} precision here.
	 *
	 * <p>It is generally not a good idea to use an ambiguous, unitless {@code long} to represent
	 * elapsed time. Therefore, we recommend using {@link #elapsed()} instead, which returns a
	 * strongly-typed {@link Duration} instance.
	 *
	 * @since 14.0 (since 10.0 as {@code elapsedTime()})
	 */
	public long elapsed(final TimeUnit desiredUnit) {
		return desiredUnit.convert(elapsedNanos(), NANOSECONDS);
	}

	/**
	 * Returns the current elapsed time shown on this stopwatch as a {@link Duration}. Unlike {@link
	 * #elapsed(TimeUnit)}, this method does not lose any precision due to rounding.
	 *
	 * @since 22.0
	 */
	public Duration elapsed() {
		return Duration.ofNanos(elapsedNanos());
	}

	/**
	 * Returns a string representation of the current elapsed time.
	 */
	@Override
	public String toString() {
		final long nanos = elapsedNanos();

		final TimeUnit unit = chooseUnit(nanos);
		final double value = (double) nanos / NANOSECONDS.convert(1, unit);

		// Too bad this functionality is not exposed as a regular method call
		return value + " " + abbreviate(unit);
	}

	private static TimeUnit chooseUnit(final long nanos) {
		if (DAYS.convert(nanos, NANOSECONDS) > 0) {
			return DAYS;
		}
		if (HOURS.convert(nanos, NANOSECONDS) > 0) {
			return HOURS;
		}
		if (MINUTES.convert(nanos, NANOSECONDS) > 0) {
			return MINUTES;
		}
		if (SECONDS.convert(nanos, NANOSECONDS) > 0) {
			return SECONDS;
		}
		if (MILLISECONDS.convert(nanos, NANOSECONDS) > 0) {
			return MILLISECONDS;
		}
		if (MICROSECONDS.convert(nanos, NANOSECONDS) > 0) {
			return MICROSECONDS;
		}
		return NANOSECONDS;
	}

	private static String abbreviate(final TimeUnit unit) {
		switch (unit) {
			case NANOSECONDS:
				return "ns";
			case MICROSECONDS:
				return "\u03bcs"; // μs
			case MILLISECONDS:
				return "ms";
			case SECONDS:
				return "s";
			case MINUTES:
				return "min";
			case HOURS:
				return "h";
			case DAYS:
				return "d";
			default:
				throw new AssertionError();
		}
	}
}
