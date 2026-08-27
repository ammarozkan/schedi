#include <schedi/epoll.h>
#include <schedi/jobs.h>
#include <schedi/log.h>

#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include <poll.h>

#define MAX_EVENTS 1024

static int epfd_read = -1;
static int epfd_write = -1;
static int epoll_wakefd = -1;
static pthread_t epoll_thread;
static pthread_mutex_t epoll_thread_lock = PTHREAD_MUTEX_INITIALIZER;
static _Atomic int epoll_thread_running = 0;
static _Atomic int epoll_stop = 0;

void schedi_epoll_del(int fd)
{
	epoll_ctl(epfd_read, EPOLL_CTL_DEL, fd, NULL);
	epoll_ctl(epfd_write, EPOLL_CTL_DEL, fd, NULL);
}

void schedi_epoll_del_read(int fd)
{
	epoll_ctl(epfd_read, EPOLL_CTL_DEL, fd, NULL);
}

void schedi_epoll_del_write(int fd)
{
	epoll_ctl(epfd_write, EPOLL_CTL_DEL, fd, NULL);
}

void schedi_epoll_del_socketcompletely(struct schedi_job_epollsocket *socket)
{
	schedi_epoll_del(socket->fd);
#ifdef SCHEDI_EXT_TIMEOUT
	schedi_epoll_del_read(socket->to_timer_fd);
#endif /*SCHEDI_EXT_TIMEOUT*/
}

int schedi_epoll_add(int fd, struct schedi_epoll_data *data, uint32_t events)
{
	int added = -1;

	if (events & EPOLLIN) {
		struct epoll_event ev = {
			.events = events & ~EPOLLOUT,
			.data = { .ptr = data },
		};
		if (epoll_ctl(epfd_read, EPOLL_CTL_ADD, fd, &ev) < 0)
			goto fail;
		added = epfd_read;
	}
	if (events & EPOLLOUT) {
		struct epoll_event ev = {
			.events = events & ~EPOLLIN,
			.data = { .ptr = data },
		};
		if (epoll_ctl(epfd_write, EPOLL_CTL_ADD, fd, &ev) < 0)
			goto fail;
		added = epfd_write;
	}
	return 0;

fail:
	if (added >= 0)
		epoll_ctl(added, EPOLL_CTL_DEL, fd, NULL);
	free(data);
	return -1;
}

int schedi_epoll_mod(int fd, struct schedi_epoll_data* data, uint32_t events)
{
	int ret = 0;

	if (events & EPOLLIN) {
		struct epoll_event ev = {
			.events = events & ~EPOLLOUT,
			.data = { .ptr = data },
		};
		ret |= epoll_ctl(epfd_read, EPOLL_CTL_MOD, fd, &ev);
	}
	if (events & EPOLLOUT) {
		struct epoll_event ev = {
			.events = events & ~EPOLLIN,
			.data = { .ptr = data },
		};
		ret |= epoll_ctl(epfd_write, EPOLL_CTL_MOD, fd, &ev);
	}

	return ret;
}

static void schedi_epoll_handle_read(struct epoll_event *ev, int epfd)
{
	struct schedi_epoll_data *d = ev->data.ptr;
	if (!d)
		return;

	int error = ev->events & (EPOLLERR | EPOLLHUP);
	int evn = ev->events;

	switch (d->type) {
	case SCHEDI_EPOLL_DATA_JOB: {
		struct schedi_job_epoll_request *req = d->as.ptr;

		schedi_epoll_del(req->socketfd);
		schedi_job_epoll_request_return(req, error);
		free(req);
		free(d);
		break;
	}
	case SCHEDI_EPOLL_DATA_SOCKET: {
		struct schedi_job_epollsocket* sock = d->as.ptr;

		if(schedi_job_epollsocket_return(sock, evn)) {
			int fd = sock->fd;
			schedi_epoll_del_socketcompletely(sock);
			free(d);
			schedi_job_epollsocket_done_(sock, 
					EPOLLSOCKET_DONE_EPOLL);
		}
		break;
	}
#ifdef SCHEDI_EXT_TIMEOUT
	case SCHEDI_EPOLL_DATA_EPOLLSOCKTO: {
		struct schedi_job_epollsocket* sock = d->as.ptr;

		// we can do this check here because everything that makes an
		// epollsocket ready is in this thread and when the execution
		// is here, there is no way for a race to be happened. So if
		// socket is timed out after being ready, this is not socket's
		// fault. Socket is a good little fella doing its job.
		if(schedi_job_epollsocket_ready(sock)) {
			schedi_job_epollsocket_timeout_reset(sock);
			break;
		}


		schedi_job_epollsocket_timeout_cas(sock);
		schedi_epoll_del_socketcompletely(sock);
		free(d);
		schedi_job_epollsocket_done_(sock,
				EPOLLSOCKET_DONE_EPOLL);
		break;
	}
#endif /*SCHEDI_EXT_TIMEOUT*/
	default:
		free(d);
		break;
	}
}

static void schedi_epoll_handle_write(struct epoll_event *ev, int epfd)
{
	struct schedi_epoll_data *d = ev->data.ptr;
	if (!d)
		return;

	int error = ev->events & (EPOLLERR | EPOLLHUP);
	int evn = ev->events;

	switch (d->type) {
	case SCHEDI_EPOLL_DATA_JOB: {
		struct schedi_job_epoll_request *req = d->as.ptr;

		schedi_epoll_del(req->socketfd);
		schedi_job_epoll_request_return(req, error);
		free(req);
		free(d);
		break;
	}
	case SCHEDI_EPOLL_DATA_SOCKET: {
		struct schedi_job_epollsocket* sock = d->as.ptr;

		if(schedi_job_epollsocket_return(sock, evn)) {
			int fd = sock->fd;
			schedi_epoll_del_socketcompletely(sock);
			free(d);
			schedi_job_epollsocket_done_(sock, 
					EPOLLSOCKET_DONE_EPOLL);
		}
		break;
	}
	default:
		free(d);
		break;
	}
}

static void schedi_epoll_drain_read(int epfd, struct epoll_event *events)
{
	for (;;) {
		int n = epoll_wait(epfd, events, MAX_EVENTS, 0);
		if (n <= 0)
			break;
		for (int i = 0; i < n; i++)
			schedi_epoll_handle_read(&events[i], epfd);
	}
}

static void schedi_epoll_drain_write(int epfd, struct epoll_event *events)
{
	for (;;) {
		int n = epoll_wait(epfd, events, MAX_EVENTS, 0);
		if (n <= 0)
			break;
		for (int i = 0; i < n; i++)
			schedi_epoll_handle_write(&events[i], epfd);
	}
}

static int schedi_epoll_init_fds(void)
{
	if (epfd_read >= 0)
		return 0;

	epfd_read = epoll_create1(0);
	if (epfd_read < 0)
		return -1;

	epfd_write = epoll_create1(0);
	if (epfd_write < 0) {
		close(epfd_read);
		epfd_read = -1;
		return -1;
	}

	epoll_wakefd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	if (epoll_wakefd < 0) {
		close(epfd_read);
		close(epfd_write);
		epfd_read = epfd_write = -1;
		return -1;
	}

	return 0;
}

int schedi_epoll_loop()
{
	if (atomic_load_explicit(&epoll_stop, memory_order_acquire))
		return 0;

	if (epfd_read < 0 || epfd_write < 0) {
		schedi_ferr("schedi_epoll_loop: epoll instances not initialized (call schedi_epoll_thread_init first)", -1);
		return -1;
	}

	struct epoll_event events[MAX_EVENTS];
	bool poll_error = false;

	while (1) {
		if (atomic_load_explicit(&epoll_stop, memory_order_acquire))
			break;

		struct pollfd pfd[3] = {
			{ .fd = epoll_wakefd, .events = POLLIN },
			{ .fd = epfd_read,    .events = POLLIN },
			{ .fd = epfd_write,   .events = POLLIN },
		};

		int r = poll(pfd, 3, -1);
		if (atomic_load_explicit(&epoll_stop, memory_order_acquire))
			break;
		if (r < 0) {
			if (errno == EINTR)
				continue;
			perror("poll");
			poll_error = true;
			break;
		}

		if (pfd[0].revents & POLLIN) {
			uint64_t one;
			while (read(epoll_wakefd, &one, sizeof(one)) > 0)
				;
		}

		if (pfd[1].revents & POLLIN)
			schedi_epoll_drain_read(epfd_read, events);
		if (pfd[2].revents & POLLIN)
			schedi_epoll_drain_write(epfd_write, events);
	}

	close(epoll_wakefd);
	epoll_wakefd = -1;
	close(epfd_read);
	epfd_read = -1;
	close(epfd_write);
	epfd_write = -1;
	if(poll_error) return -1;
	return 0;
}

static void *schedi_epoll_thread_routine(void *arg)
{
	schedi_flog("epoll thread routine entered", 0);
	int ret = schedi_epoll_loop();
	schedi_flog("epoll thread routine exited (code:return)", ret);
	return NULL;
}

int schedi_epoll_thread_init(void)
{
	int err = 0;

	pthread_mutex_lock(&epoll_thread_lock);
	if (atomic_load_explicit(&epoll_thread_running, memory_order_acquire)) {
		pthread_mutex_unlock(&epoll_thread_lock);
		return 0;
	}

	if (schedi_epoll_init_fds() < 0) {
		schedi_ferr("creating epoll instances failed on schedi_epoll_thread_init (code:errno)", errno);
		pthread_mutex_unlock(&epoll_thread_lock);
		return -1;
	}

	err = pthread_create(&epoll_thread, NULL,
	                     schedi_epoll_thread_routine, NULL);
	if (err) {
		schedi_ferr("pthread_create failed on schedi_epoll_thread_init (code:err)", err);
		pthread_mutex_unlock(&epoll_thread_lock);
		return -1;
	}

	atomic_store_explicit(&epoll_thread_running, 1, memory_order_release);
	pthread_mutex_unlock(&epoll_thread_lock);
	return 0;
}

int schedi_epoll_thread_deinit(void)
{
	int ret = 0;

	pthread_mutex_lock(&epoll_thread_lock);
	if (!atomic_load_explicit(&epoll_thread_running, memory_order_acquire)) {
		pthread_mutex_unlock(&epoll_thread_lock);
		return 0;
	}

	atomic_store_explicit(&epoll_stop, 1, memory_order_release);

	uint64_t one = 1;
	if (epoll_wakefd >= 0)
		(void)write(epoll_wakefd, &one, sizeof(one));

	ret = pthread_join(epoll_thread, NULL);
	if (ret)
		schedi_ferr("schedi_epoll_thread_deinit, couldnt join. (code:err)", ret);

	atomic_store_explicit(&epoll_thread_running, 0, memory_order_release);
	pthread_mutex_unlock(&epoll_thread_lock);
	return ret;
}

int schedi_epoll_destroy(void)
{
	pthread_mutex_lock(&epoll_thread_lock);
	if (epfd_read >= 0) {
		close(epfd_read);
		epfd_read = -1;
	}
	if (epfd_write >= 0) {
		close(epfd_write);
		epfd_write = -1;
	}
	if (epoll_wakefd >= 0) {
		close(epoll_wakefd);
		epoll_wakefd = -1;
	}
	pthread_mutex_unlock(&epoll_thread_lock);
	return 0;
}
