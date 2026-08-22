#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>
#include <schedi/epoll.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define CLIENT_COUNT 4
#define THREAD_COUNT 2
#define PORT 5006

struct job_context {
	int fd;
	struct schedi_job_epollsocket* socket;
};

struct job_context ctxs[CLIENT_COUNT];
struct schedi_job* jobs[CLIENT_COUNT];
struct schedi_job_completion_indicator indicators[CLIENT_COUNT];

void dtor(void* context)
{
	struct job_context* ctx = (struct job_context*)context;
	schedi_job_epollsocket_done(ctx->socket);
	close(ctx->fd);
}

int job_func(struct schedi_job* job)
{
	struct job_context* ctx = (struct job_context*)job->context;

	switch(job->phase) {
	case 0: {
		ctx->socket = schedi_job_tool_epollsocket(job, ctx->fd, 1, 0);
		schedi_job_required_available_socket(job, 1);
		job->phase = 1;
		return 1;
	}
	case 1: {
		char buf[256] = {0};
		int n = schedi_job_epollsocket_read(ctx->socket, buf, sizeof(buf) - 1);
		if(n > 0) {
			buf[n] = '\0';
			printf("server: read %d bytes: \"%s\"\n", n, buf);
			schedi_job_epollsocket_write(ctx->socket, buf, n);
			job->phase = 2;
			return 1;
		}
		if(ctx->socket->htc & SCHEDI_JOB_EPOLLSOCKET_DEAD) {
			printf("server: client disconnected (DEAD)\n");
			return -1;
		}
		return 1;
	}
	case 2: {
		if(ctx->socket->htc & SCHEDI_JOB_EPOLLSOCKET_DEAD) {
			printf("server: client disconnected after echo\n");
			return -1;
		}
		return 1;
	}
	}
	return -1;
}

#include "create_server.h"
#include "accept_client.h"

void client_func(int id)
{
	usleep(20000);

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if(fd < 0) {
		perror("client socket");
		_exit(1);
	}

	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(PORT),
	};
	inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

	if(connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
		perror("client connect");
		_exit(1);
	}

	char msg[64];
	snprintf(msg, sizeof(msg), "hello from client %d", id);

	write(fd, msg, strlen(msg));
	printf("client %d: sent \"%s\"\n", id, msg);

	char buf[256] = {0};
	int n = read(fd, buf, sizeof(buf) - 1);
	if(n > 0) {
		buf[n] = '\0';
		if(strcmp(buf, msg) == 0)
			printf("client %d: echo OK\n", id);
		else
			printf("client %d: echo mismatch! got \"%s\"\n", id, buf);
	} else {
		printf("client %d: no echo received\n", id);
	}

	close(fd);
	printf("client %d: done\n", id);
}

int main()
{
	signal(SIGCHLD, SIG_IGN);
	setbuf(stdout, NULL);

	if(schedi_job_init()) return 1;
	if(schedi_epoll_thread_init()) return 3;
	if(schedi_worker_init(THREAD_COUNT)) return 2;

	int serverfd = create_server_nonblock("0.0.0.0", PORT);
	if(serverfd < 0) {
		printf("Server creation error\n");
		return 1;
	}

	printf("server: listening on port %d\n", PORT);

	for(int i = 0; i < CLIENT_COUNT; i++) {
		pid_t pid = fork();
		if(pid == 0) {
			client_func(i);
			_exit(0);
		}
	}

	for(int i = 0; i < CLIENT_COUNT; i++) {
		int fd = -1;
		while(fd < 0) {
			fd = accept_client_nonblock(serverfd);
			if(fd < 0) usleep(1000);
		}
		ctxs[i].fd = fd;
		ctxs[i].socket = NULL;
		jobs[i] = schedi_job_create((void*)&ctxs[i], job_func, dtor);
		schedi_completion_indicator_init(&indicators[i]);
		jobs[i]->completion_indicator = &indicators[i];
		schedi_job_setready(jobs[i]);
	}

	for(int i = 0; i < CLIENT_COUNT; i++) {
		schedi_job_completion_wait(&indicators[i]);
	}

	printf("All jobs completed.\n");

	if(schedi_worker_deinit()) return 4;
	if(schedi_job_deinit()) return 5;

	printf("PASS\n");
	return 0;
}
