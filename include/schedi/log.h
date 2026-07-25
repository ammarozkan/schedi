#ifndef SCHEDI_LOG_H
#define SCHEDI_LOG_H

#include <stdint.h>
#include <sys/types.h>

//#define SCHEDI_LOG_NAMED_THREADS 1 	// enables naming threads to make them more human readible
									// spends extra instructions on selecting names and comparing
									// if its a new thread or not

/* Ring buffer size — power of two for cheap modulo via & mask */
#define SCHEDI_FLOG_SIZE 4096

/**
 * struct schedi_flog_event - A single fast-log event stored in memory.
 * @tid:  Thread ID that logged the event.
 * @msg:  Short C-string describing the event (must persist).
 * @param: Auxiliary integer parameter (e.g. fd, error code, counter).
 *
 * Intended for lock-free ring-buffer logging; written by the producer,
 * read asynchronously by a consumer (e.g. crash dump or stats reporter).
 * The caller guarantees that @msg points to storage that outlives the
 * buffer (typically a string literal or static storage).
 */
struct schedi_flog_event {
	pid_t tid;
#ifdef SCHEDI_LOG_NAMED_THREADS
	const char* name;
#endif /*SCHEDI_LOG_NAMED_THREADS*/
	const char *msg;
	int param;
};

/**
 * schedi_flog() - Append a fast-log event to the in-memory ring buffer.
 * @msg:   Short description string (must be or outlive the caller).
 * @param: Arbitrary integer to associate with the event.
 *
 * Stores a &struct schedi_flog_event at the current write position in an
 * internal lock-free ring buffer.  Intended for hot paths where
 * printf-style logging is too expensive.
 */
void schedi_flog(const char *msg, int param);

// events should point to a buffer thats at least SCHEDI_FLOG_SIZE.
// events[4095] is the last event. If events are full, events[0]
// would be the oldest event that is still saved.
void schedi_flog_get(struct schedi_flog_event* events);


// Same functions, just for error category. Also logs to flog at the same time.

void schedi_ferr(const char* msg, int param);
void schedi_ferr_get(struct schedi_flog_event* events);

#endif /* SCHEDI_LOG_H */
