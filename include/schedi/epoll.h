#ifndef SCHEDI_EPOLL_H
#define SCHEDI_EPOLL_H

#include <sys/epoll.h>
#include <stdint.h>

/**
 * SCHEDI_EPOLL_DATA_NONE - reserved, no handler
 * SCHEDI_EPOLL_DATA_SVLISTEN - Server listen fd, as.fd
 * SCHEDI_EPOLL_DATA_JOB - 	one time request by job. removes from epoll
 * 							list after handled.
 * SCHEDI_EPOLL_DATA_SOCKET - 	long time socket. Only in specified
 * 								conditions it handled. Unless, received
 * 								data will only be stored in the ring
 * 								buffer
 */
#define SCHEDI_EPOLL_DATA_NONE      0
#define SCHEDI_EPOLL_DATA_SVLISTEN  1
#define SCHEDI_EPOLL_DATA_JOB       2
#define SCHEDI_EPOLL_DATA_SOCKET	3

/**
 * struct schedi_epoll_data - Tagged union for epoll event dispatch.
 * @type: Discriminator (SCHEDI_EPOLL_DATA_SVLISTEN or SCHEDI_EPOLL_DATA_JOB).
 * @as: Union — @as.fd when type is LISTEN, @as.ptr when type is JOB.
 */
struct schedi_epoll_data {
	uint8_t type;
	union {
		int fd;
		void *ptr;
	} as;
};

/**
 * schedi_epoll_loop - Run the main epoll event loop.
 *
 * Creates an epoll fd, and
 * loops on epoll_wait indefinitely. Dispatches events via tagged
 * &struct schedi_epoll_data. Never returns unless epoll_wait fails.
 *
 * Return: 0 on normal exit, -1 on error.
 */
int schedi_epoll_loop();

/**
 * schedi_epoll_add - Register an fd with the main epoll (thread-safe).
 * @fd: File descriptor to watch.
 * @data: Tagged data handed back via epoll_event.data.ptr on event.
 * @events: epoll events mask (EPOLLIN | EPOLLET | ...).
 *
 * Safe to call from any thread — epoll_ctl is thread-safe in the kernel.
 *
 * Return: 0 on success, -1 on error.
 */
int schedi_epoll_add(int fd, struct schedi_epoll_data *data, uint32_t events);

#endif /* SCHEDI_EPOLL_H */
