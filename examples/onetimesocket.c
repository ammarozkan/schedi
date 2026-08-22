/**
 * This example provides a one time socket usage on a job.
 * When socket received some action, socket will be marked
 * ready. When all requested sockets receive some action,
 * job will be marked ready if job does not waits some other
 * stuff too.
 * 
 * In this example, job will be listening for some new
 * connections. When a new connection comes up, it will
 * read from there. Also there are JOB_COUNT amount of
 * that jobs with different ports from 1997, 1998 to
 * 1997+JOB_COUNT-1.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/epoll.h> // for epoll flags
	
#include <schedi/worker.h>
#include <schedi/jobs.h>

#include <schedi/epoll.h>

#include <errno.h>

#define JOB_COUNT 4

struct job_context {
	int serverfd;
	int clientfd;
	uint16_t port;

	bool server_created;
	bool client_accepted;
};


struct job_context context[JOB_COUNT];
struct schedi_job_completion_indicator indicator[JOB_COUNT];

#include "create_server.h"
#include "accept_client.h"


int job_func(struct schedi_job* job)
{
	struct job_context* context = (struct job_context*)job->context;

	printf("Total Req:%u\t\tWaiting:%u\n", job->epoll_list->total_req,
		SCHEDI_JOB_METAFLAG_GETWAIT(atomic_load_explicit(&job->meta_flag, memory_order_relaxed)));
	printf("Ret Count:%u\t\tError Count:%u\n", job->epoll_list->ret_count, job->epoll_list->err_count);

	if(!context->server_created) {
		context->serverfd = create_server_nonblock("0.0.0.0", context->port);
		if(context->serverfd < 0) {
			printf("Unsuccesfull server. %i\n", context->serverfd);
			return 0;
		}
		else {
			printf("Server:%i\n", context->serverfd);
			context->server_created = true;
		}
		printf("Epoll Tool:%i\n", schedi_job_tool_epoll(job, context->serverfd, EPOLLIN));
		return 1;
	}

	if(!context->client_accepted) {
		context->clientfd = accept_client_nonblock(context->serverfd);
		printf("Client accept:%i\n", context->clientfd);
		if(context->clientfd == -1) {
			printf("Unsuccesfull client.\n"); 
			printf("Epoll Tool:%i\n", schedi_job_tool_epoll(job, context->serverfd, EPOLLIN));
			return 1;
		} else {
			context->client_accepted = true;
		}
		printf("Epoll Tool:%i\n", schedi_job_tool_epoll(job, context->clientfd, EPOLLIN));
		return 1;
	}

	char buf[16];
	read(context->clientfd, buf, 6);
	printf("Client Input:'%s'\n", buf);

	// be completeth
	close(context->serverfd);
	close(context->clientfd);
	return -1;
}


struct schedi_job* get_job(uint16_t port, int indice)
{
	context[indice].port = port;
	struct schedi_job* job = schedi_job_create(&context[indice], job_func, NULL);
	schedi_completion_indicator_init(&indicator[indice]);
	job->completion_indicator = &indicator[indice];
	schedi_job_setready(job);
	return job;
}


int main()
{

	if(schedi_job_init()) {
		printf("Job init fail.\n");
		return 1;
	}

	if(schedi_epoll_thread_init()) {
		printf("Epoll thread init fail.\n");
		return 2;
	}

	if(schedi_worker_init(2)) {
		printf("Worker init fail.\n");
		return 3;
	}

	struct schedi_job* jobs[JOB_COUNT];

	for(unsigned int i = 0 ; i < JOB_COUNT ; i += 1) {
		printf("JOB PORT:%i\n", 1997+i);
		jobs[i] = get_job(1997+i, i);
	}

	for(unsigned int i = 0 ; i < JOB_COUNT ; i += 1) {
		schedi_job_completion_wait(&indicator[i]);
	}

	printf("Completed.\n");

	return 0;

	//thread dinit is faulty but it would be here.
}
