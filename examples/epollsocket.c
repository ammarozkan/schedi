/**
 * This program gives an example to how to use buffer based socket system
 * with epoll on background.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>
#include <schedi/epoll.h>

#include <unistd.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <stdatomic.h>


#define ACCEPT_COUNT 4
#define THREAD_COUNT 2

struct job_context {
	int fd;
	struct schedi_job_epollsocket* socket;
	bool exit;
};

struct job_context ctxs[ACCEPT_COUNT];
struct schedi_job* jobs[ACCEPT_COUNT];
struct schedi_job_completion_indicator completion_indicators[ACCEPT_COUNT];

void dtor(void* context)
{
	struct job_context* ctx = (struct job_context*)context;
	close(ctx->fd);
}

int job_func(struct schedi_job* job)
{
	struct job_context* ctx = (struct job_context*)job->context;

	if(ctx->exit) {
		return -1;
	}

	if(ctx->socket == NULL) {
#ifdef SCHEDI_EXT_TIMEOUT
		ctx->socket = schedi_job_tool_epollsocket(job, ctx->fd, 20, 0, 3);
#else
		ctx->socket = schedi_job_tool_epollsocket(job, ctx->fd, 20, 0);
#endif /*SCHEDI_EXT_TIMEOUT*/
		schedi_job_required_available_socket(job, 1);
		return 1;
	}

	uint64_t htc = atomic_load(&ctx->socket->htc);

	if(htc & SCHEDI_JOB_EPOLLSOCKET_DEAD) {
		printf("Socket is dead.\n");
		schedi_job_epollsocket_done(ctx->socket);
		ctx->exit = true;
		return 1;
	}
	if(htc & SCHEDI_JOB_EPOLLSOCKET_EPOLLDID) {
		printf("Socket is epolldid.\n");
	}

#ifdef SCHEDI_EXT_TIMEOUT
	if(htc & SCHEDI_JOB_EPOLLSOCKET_TIMEOUT) {
		printf("Socket is timed out.\n");
		char b;
		while(recv(ctx->socket->to_timer_fd, &b, 1, MSG_DONTWAIT) > 0)
			printf("Reading timer_fd.\n");
	}

	if(schedi_job_epollsocket_timeout(ctx->socket)) {
		printf("Timed out.\n");
		if(schedi_job_epollsocket_epollagain(ctx->socket)) {
			printf("Epoll not again.\n");
			schedi_job_epollsocket_done(ctx->socket);
			ctx->exit = true;
			return 1;
		} else {
			printf("Epoll again!\n");
			return 1;
		}
	} else {
		printf("Not timed out.\n");
	}
#endif /*SCHEDI_EXT_TIMEOUT*/


	char data[32];
	
	uint32_t rcc = schedi_job_epollsocket_read(ctx->socket, data, 20);
	if(rcc > 0)
		printf("Readen(%u) data:%s\n", rcc, data);
	else if(ctx->socket->htc & SCHEDI_JOB_EPOLLSOCKET_DEAD) {
		schedi_job_epollsocket_done(ctx->socket);
		ctx->exit = true;
		return 1;
	}
	
	return 1;
}


#include "create_server.h"
#include "accept_client.h"

int main()
{
	if(schedi_job_init()) {
		return 1;
	}

	if(schedi_epoll_thread_init()) {
		return 3;
	}

	if(schedi_worker_init(THREAD_COUNT)) {
		return 2;
	}

	int serverfd = create_server("0.0.0.0", 5005);
	if(serverfd < 0) {
		printf("Server FD creation error.\n");
	}

	for(unsigned int i = 0 ; i < ACCEPT_COUNT ; i += 1) {
		ctxs[i].fd = accept_client(serverfd);
		if(ctxs[i].fd < 0) {
			printf("Accept client error.\n");
			continue;
		}
		jobs[i] = schedi_job_create((void*)&ctxs[i], job_func, dtor);
		schedi_completion_indicator_init(&completion_indicators[i]);
		jobs[i]->completion_indicator = &completion_indicators[i];

		schedi_job_setready(jobs[i]);
	}

	for(unsigned int i = 0 ; i < ACCEPT_COUNT ; i += 1) {
		schedi_job_completion_wait(&completion_indicators[i]);
	}

	if(schedi_worker_deinit()) {
		printf("Worker deinit failed.\n");
		return 4;
	}

	if(schedi_job_deinit()) {
		printf("Job deinit failed.\n");
		return 5;
	}

	printf("End of the road.\n");
	return 0;
}
