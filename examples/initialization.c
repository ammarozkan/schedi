/**
 * This file shows an example to how to initialize and deinitialize
 * the system.
 */


#include <stdio.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>

#include "logprinter.h"


int main()
{
	if(schedi_job_init()) {
		LogPrint();
		return 1;
	}

	// initializing 16 worker thread for testing
	if(schedi_worker_init(16)) {
		LogPrint();
		return 2;
	}
	
	schedi_wake_all_job_waiters();

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