#ifndef SCHEDI_JOBS_EPOLL_SOCKET_H
#define SCHEDI_JOBS_EPOLL_SOCKET_H

/**
 * This file contains the definitions for the registered socket system.
 * Registered sockets will be able to written by the epoll loop and readen
 * by various consumers.
 */


#define SCHEDI_JOB_EPOLL_SOCKET_READY (1<<15)


struct schedi_job_epoll_socket {
	int fd;
	_Atomic uint64_t htc; 	// highest 16 is head, then tail, then count, then 1 bit indicates if its ready
							// or not. Last 15 is label.
	char buffer[1024];
	_Atomic uint32_t count_condition;	// when count passes this,
										// the socket will be ready
};


/**
 * Writes to socket atomically. Only 1 thread can call this.
 * 
 * Returns written byte count.
 */

int schedi_job_epoll_socket_write(struct schedi_job_epoll_socket* socket, char* data, size_t size);

int schedi_job_epoll_socket_read(struct schedi_job_epoll_socket* socket, char* data, size_t size);

int schedi_job_epoll_socket_isready(struct schedi_job_epoll_socket* socket);

#endif /* SCHEDI_JOBS_EPOLL_SOCKET_H */