#ifndef SCHEDI_EPOLL_H
#define SCHEDI_EPOLL_H

#include <sys/epoll.h>
#include <stdint.h>

/**
 * SCHEDI_EPOLL_DATA_NONE - Reserved, no handler.
 * SCHEDI_EPOLL_DATA_JOB - One time request by job. removes from epoll
 * 			   list after handled.
 * SCHEDI_EPOLL_DATA_SOCKET - 	long time socket. It is handled in specific
 * 				conditions. Otherwise, received data will only 
 * 				be stored in the ring buffer.
 * SCHEDI_EPOLL_DATA_EPOLLSOCKTO - Timeout for an epollsocket.
 */
#define SCHEDI_EPOLL_DATA_NONE      0
#define SCHEDI_EPOLL_DATA_JOB       1
#define SCHEDI_EPOLL_DATA_SOCKET	2
#define SCHEDI_EPOLL_DATA_EPOLLSOCKTO	101

/**
 * struct schedi_epoll_data - Tagged union for epoll event dispatch.
 * @type: Discriminator (SCHEDI_EPOLL_DATA_JOB, SOCKET or NONE).
 * @as: Union — @as.ptr when type is JOB or SOCKET.
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
 * Loops on poll()/epoll_wait indefinitely. Dispatches events via tagged
 * &struct schedi_epoll_data. Never returns unless poll() fails or epoll_stop
 * is set.
 *
 * The epoll instances must already exist: call schedi_epoll_thread_init()
 * first, or loop on the read/write epoll fds directly.
 *
 * Return: 0 on normal exit, -1 on error.
 */
int schedi_epoll_loop();

/**
 * schedi_epoll_thread_init - Start the epoll system in a background thread.
 *
 * Creates the read/write epoll instances and the wake eventfd, then spawns a
 * dedicated thread that runs schedi_epoll_loop() indefinitely, so the caller
 * can return and do other work while epoll events are dispatched.
 *
 * Return: 0 on success, -1 on failure.
 */
int schedi_epoll_thread_init(void);

/**
 * schedi_epoll_thread_deinit - Stop the epoll thread and join it.
 *
 * Marks an atomic stop flag and wakes the pool loop via an eventfd, so a
 * blocking pool() returns and the loop exits. Waits for the thread to
 * finish (pthread_join). Thread-safe; concurrent calls are serialized.
 *
 * Return: 0 on success, errno on failure.
 */
int schedi_epoll_thread_deinit(void);

/**
 * schedi_epoll_destroy - Close the epoll instance and wake fd.
 *
 * Closes the epoll and eventfd descriptors if still open. Should be called
 * after schedi_epoll_thread_deinit(). Thread-safe.
 *
 * Return: 0.
 */
int schedi_epoll_destroy(void);

/**
 * schedi_epoll_add() - Register an fd with the main epoll (thread-safe).
 * @fd: File descriptor to watch.
 * @data: Tagged data handed back via epoll_event.data on event. An epoll_event
 * data passed as this variable shall not be freed. From this call on, freeing 
 * of this data belongs to the epoll system no matter what the function 
 * returns.
 * @events: epoll events mask (EPOLLIN | EPOLLET | ...).
 *
 * Safe to call from any thread — epoll_ctl is thread-safe in the kernel.
 *
 * Function only registers to read and write specified epolls, seperating the
 * read-write events. If EPOLLIN nor EPOLLOUT not set, the call won't do 
 * anything.
 *
 * Return: 0 on success, -1 on error.
 */
int schedi_epoll_add(int fd, struct schedi_epoll_data *data, uint32_t events);

/**
 * schedi_epoll_del() - Remove an fd from every epoll instance.
 * @fd: file descriptor to remove.
 *
 * Deletes @fd from both the read and write epoll instances. Errors are
 * ignored (the fd may not be registered in one of them).
 */
void schedi_epoll_del(int fd);

/**
 * schedi_epoll_del_read() - Remove an fd from the read epoll instance.
 * @fd: file descriptor to remove.
 */
void schedi_epoll_del_read(int fd);

/**
 * schedi_epoll_del_write() - Remove an fd from the write epoll instance.
 * @fd: file descriptor to remove.
 */
void schedi_epoll_del_write(int fd);

/**
 * schedi_epoll_mod() - Epoll mod call.
 * @fd: Socket file descriptor.
 * @data: Data that is used with that epoll entry.
 * @events: Events to update.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_epoll_mod(int fd, struct schedi_epoll_data* data, uint32_t events);

#endif /* SCHEDI_EPOLL_H */
