#ifndef SCHEDI_WORKER_H
#define SCHEDI_WORKER_H

#include <pthread.h>

#define SCHEDI_MAX_WORKERS 512
#define SCHEDI_DEFAULT_WORKER_COUNT 4

/**
 * struct schedi_worker_ctx - Per-worker thread context.
 * @id:      Worker index (0-based).
 * @thread:  POSIX thread handle.
 * @shutdown:  Non-zero signals the worker to exit its loop.
 * @alive:     Set to 1 while the thread is running, 0 after it exits.
 *
 * Allocated statically in an array by &schedi_worker_init. Each worker
 * spins on @shutdown and sets @alive = 0 just before returning.
 */
struct schedi_worker_ctx {
	int id;
	pthread_t thread;
	_Atomic int shutdown;
	_Atomic int alive;
};

/**
 * schedi_worker_init() - Spawn a fixed number of worker threads.
 * @worker_count:  Number of workers to create (capped at %SCHEDI_MAX_WORKERS).
 *
 * Initialises &schedi_worker_ctx slots 0..worker_count-1, sets @shutdown = 0,
 * @alive = 1, and calls pthread_create for each.
 *
 * Return: 0 on success, a positive errno value if pthread_create fails.
 */
int schedi_worker_init(int worker_count);

/**
 * schedi_worker_initd() - Spawn the default number of worker threads.
 *
 * Shorthand for schedi_worker_init(SCHEDI_DEFAULT_WORKER_COUNT). Workers
 * spin until signalled by &schedi_worker_deinit.
 *
 * Return: 0 on success, a positive errno value on failure.
 */
int schedi_worker_initd(void);

/**
 * schedi_worker_deinit() - Signal all workers to stop and join them.
 *
 * Sets @shutdown = 1 on every worker, then pthread_joins each one.
 * Safe to call even if no workers were ever started (worker_count == 0).
 *
 * Return: 0 on success, the last errno from pthread_join otherwise.
 */
int schedi_worker_deinit(void);

#endif /* SCHEDI_WORKER_H */
