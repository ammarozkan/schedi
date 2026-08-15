/**
 * problem is to get 2 vectors, set them in memory and set one of
 * the vectors, the dot product of them. and do all that for 
 * 1 million of two vectors.
 * 
 * All the required stuff for this sample problem is at 
 * aparalleljob_parameters.h. The comparison single-thread version
 * solution also uses the same file for the problem.
 */



#include <stdio.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>
#include <stdatomic.h>
#include <stdlib.h> // malloc, random
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>

#include "logprinter.h"

#include "aparalleljob_parameters.h"

struct job_context {
	int proc_indice; // 0 to PARALLEL_JOB_COUNT-1
};


// Im gonna define 32 jobs for a 1 million vector dot multiplication.
struct schedi_job* jobs[PARALLEL_JOB_COUNT];
struct schedi_job_completion_indicator completion_indicators[PARALLEL_JOB_COUNT];
struct job_context contexts[PARALLEL_JOB_COUNT];

int start_end[PARALLEL_JOB_COUNT][2];


struct job_context* GetContext(int proc_indice)
{
	struct job_context* context = 
		(struct job_context*) malloc(sizeof(struct job_context));

	context->proc_indice = proc_indice;
	return context;
}


int job_func (struct schedi_job* job)
{
	struct job_context* context = (struct job_context*)job->context;

	schedi_flog("job me entered",context->proc_indice);
	for(unsigned int i = start_end[context->proc_indice][0]; 
		i <= start_end[context->proc_indice][1] ; i += 1) {
		vecs[i] = RANDOM_VECTOR; params[i] = RANDOM_VECTOR;
		vecs[i] = DotProduct(vecs[i], params[i]);
	}

	usleep(context->proc_indice*1);

	schedi_flog("job me exiting",context->proc_indice);

	return -1; // let the job destroy. its over.
}



int main()
{
	start_end[0][0] = 0;
	start_end[0][1] = (int)VECTOR_COUNT / PARALLEL_JOB_COUNT;
	for(unsigned int i = 1 ; i < PARALLEL_JOB_COUNT-1 ; i += 1) {
		start_end[i][0] = start_end[i-1][1];
		start_end[i][1] = start_end[0][1]*(i+1);
	}

	start_end[PARALLEL_JOB_COUNT-1][0] = start_end[PARALLEL_JOB_COUNT-2][1];
	start_end[PARALLEL_JOB_COUNT-1][1] = VECTOR_COUNT-1;
	printf("Initialized.\n");


	if(schedi_job_init()) {
		LogPrint();
		return 1;
	}

	// jobs can be defined here as jobs are initialized

	for(unsigned int i = 0 ; i < PARALLEL_JOB_COUNT ; i += 1) {
		contexts[i].proc_indice = i;
		jobs[i] = schedi_job_create(
			(void*)&contexts[i], job_func, NULL);

		schedi_completion_indicator_init(&completion_indicators[i]);
		jobs[i]->completion_indicator = &completion_indicators[i];

		schedi_job_setready(jobs[i]);
	}

	// it should not get picked

	if(schedi_worker_init(THREAD_COUNT)) {
		LogPrint();
		return 2;
	}

	for(unsigned int i = 0 ; i < PARALLEL_JOB_COUNT ; i += 1) {
		schedi_job_completion_wait(&completion_indicators[i]);
		//while(!completion_indicators[i].completion);
		//printf("Job %u wait done.\n",i);
	}

	return 0; // deinit is faulty on lockly system.
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
