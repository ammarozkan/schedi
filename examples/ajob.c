#include <stdio.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>
#include <stdatomic.h>
#include <stdlib.h> // malloc
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>

#include "logprinter.h"

struct schedi_job_completion_indicator completion_indicator;

struct custom_job_context {
	int inp;
	int result;
	unsigned int runs;
};

struct custom_job_context* GetContext(int inp)
{
	struct custom_job_context* context = 
		(struct custom_job_context*) malloc(sizeof(struct custom_job_context));

	context->inp = inp;
	return context;
}


pid_t last_thread = 0;


int job_func (struct schedi_job* job)
{
	struct custom_job_context* context = (struct custom_job_context*)job->context;
	context->runs += 1;
	pid_t current_pid_t = syscall(SYS_gettid);
	if(current_pid_t != last_thread) {
		printf("Thread changed to %i on %uth run.\n", current_pid_t, context->runs);
		last_thread = current_pid_t;
	}

	switch(job->phase) {
	case 0:
		context->result = context->inp % 7 + context->inp % 5 + context->inp % 3;
		job->phase = 1;
		printf("First execution OK\n");
		break;
	case 1:
		context->result += 1;

		if (context->result % 625 == 0) goto _completed;
		schedi_flog("Calculation done.", (int)context->result);
		break;
	}

	return 1; // next execution should be ready after job is handled

_completed:
	printf("Completed\n");
	return -1; // so it gets removed
}



int main()
{
	if(schedi_job_init()) {
		LogPrint();
		return 1;
	}

	// jobs can be defined here as jobs are initialized

	struct custom_job_context* context = GetContext(6);

	struct schedi_job* job1 = schedi_job_create(
		(void*)context, job_func, NULL
	);
	schedi_completion_indicator_init(&completion_indicator);
	job1->completion_indicator = &completion_indicator;
	schedi_job_setready(job1); // making job ready

	// it should not get picked

	// initializing 16 worker thread for testing
	if(schedi_worker_init(16)) {
		LogPrint();
		return 2;
	}
	
	schedi_job_completion_wait(&completion_indicator);
	
	schedi_wake_all_job_waiters();


	return 0;
	if(schedi_worker_deinit()) {
		LogPrint();
		printf("Worker Deinit Failed\n");
		return 3;
	}

	if(schedi_job_deinit()) {
		LogPrint();
		printf("Job Deinit Failed\n");
		return 4;
	}

	LogPrint();

	printf("End of the road.\n");
	return 0;
}