#include <schedi/epoll.h>
#include <schedi/jobs.h>

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/socket.h>

#define MAX_EVENTS 1024

static int epfd;

int schedi_epoll_add(int fd, struct schedi_epoll_data *data, uint32_t events)
{
	struct epoll_event ev = {
		.events = events,
		.data = { .ptr = data },
	};


	int ret = epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

	if (ret) {
		free(data);
	}

	return ret;
}

int schedi_epoll_mod(int fd, struct schedi_epoll_data* data, uint32_t events)
{
	struct epoll_event ev = {
		.events = events,
		.data = { .ptr = data },
	};
	int ret = epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);

	return ret;
}

int schedi_epoll_loop()
{
	epfd = epoll_create1(0);
	if (epfd < 0) {
		perror("epoll_create1");
		return -1;
	}

	struct epoll_event events[MAX_EVENTS];

	while (1) {
		int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			perror("epoll_wait");
			break;
		}

		for (int i = 0; i < n; i++) {
			struct schedi_epoll_data *d = events[i].data.ptr;
			if (!d)
				continue;

			// maybe just remove error and just pass the whole events??
			int error = events[i].events & (EPOLLERR | EPOLLHUP);
			int evn = events[i].events;

			/*
			 * Every schedi_epoll_data must be completely freed here except
			 * it is a job_epoll_socket. Then do not remove it from epoll
			 * list unless the return function returns non-zero that indicates
			 * it needs to be removed now.
			 */

			switch (d->type) {
			case SCHEDI_EPOLL_DATA_JOB: {
				struct schedi_job_epoll_request *req = d->as.ptr;

				schedi_job_epoll_request_return(req, error);
				epoll_ctl(epfd, EPOLL_CTL_DEL, req->socketfd, NULL);
				free(req);
				free(d);
				break;
			}
			case SCHEDI_EPOLL_DATA_SOCKET: {
				struct schedi_job_epoll_socket* sock = d->as.ptr;

				if(schedi_job_epoll_socket_return(sock, evn)) {
					// capture the fd before releasing the epoll side's
					// reference: done() may free the socket if this was
					// the last one.
					int fd = sock->fd;
					schedi_job_tool_epollsocket_done(sock);
					epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
					free(d);
				}
				break;
			}
			default:
				free(d);
				break;
			}
		}
	}

	close(epfd);
	return 0;
}
