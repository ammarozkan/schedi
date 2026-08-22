#include <schedi/log.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>

#ifdef LOCKLESS_READYJOB
#ifdef SCHEDULE_NONBUSY
#include <sched.h>
#endif /* SCHEDULE_NONBUSY */
#endif /* LOCKLESS_READYJOB */

static struct schedi_worker_ctx workers[SCHEDI_MAX_WORKERS];
static int worker_count;

static void *worker_routine(void *arg)
{
	schedi_flog("worker_routine start",0);
	struct schedi_worker_ctx *ctx = arg;

	while (!ctx->shutdown) {

		// if thread was here (line 15) when shutdown marked and then 
		// waitforjob is signaled, it will miss all those completely and
		// hit sleep. thus thread couldnt join. ISSUE_WORKER_SHUTDOWN_RACE

		struct schedi_job *job = NULL;

		job = schedi_job_pickready();
		if(job) goto _got_a_job; 	// skip to the action without touching to
									// sleep if job is gotten
#ifdef LOCKLESS_READYJOB
		goto _continue;
#else

		schedi_pickingreadyjob();
		job = schedi_job_pickready();
		if(!job) {
			if(ctx->shutdown) {
				schedi_pickedreadyjob();
				break;
			}
			schedi_waitforjob();

			goto _continue;
		} else {
			schedi_flog("worker_routine schedi_pickedreadyjob()", 0);
			schedi_pickedreadyjob();
		}

#endif /* LOCKLESS_READYJOB */

_got_a_job:
		job->state = schedi_job_state_working;
		if(schedi_job_mark_executing(job)) goto _continue; 	// somebody stole the job. ok.
									// as stealing is illegal, this
									// check can be removed.
		schedi_job_unmark_access(job);	// holding the access from pickready.
		int ret = job->run(job);
		job->state = schedi_job_state_suspended;
		schedi_job_unmark_executing(job); 	// as with success marking execute,
							// it will success on unmarking.
		if(ret < 0) {
			schedi_flog("schedi_job_fn returned <0. Indicating completion.", 0);
			schedi_job_completion_indicate(job);

			if(job->requested_job) {
				struct schedi_job *rjob = job->requested_job;
				if(schedi_job_mark_access(rjob) == 0) {
					if(rjob->gen == job->requested_job_gen)
						schedi_job_add_available_job(rjob);
					schedi_job_unmark_access(rjob);
				}
			}

			schedi_flog("Triggering destroy.",0);
			ret = schedi_job_destroy(job);
			schedi_flog("schedi_job_destroy called. (code:schedi_job_destroy(job))",ret);
		} else if(ret == 1) { 
			schedi_job_mark_readybasic(job);
		}

_continue:
		schedi_flog("worker_routine, calling destroy_queue_check.",0);
		schedi_job_destroy_queue_check();
		schedi_flog("worker_routine destroy_queue_check called",0);

		schedi_flog("ticking reorganization", 0);
		schedi_ready_jobs_cache_reorganize_tick();
		schedi_flog("ticked reorganization", 0);

#ifdef LOCKLESS_READYJOB
#ifdef SCHEDULE_NONBUSY
		sched_yield(); // on linux, always success
#endif /* SCHEDULE_NONBUSY */
#endif /* LOCKLESS_READYJOB */

	}

	schedi_flog("shutting down this worker",0);

	ctx->alive = 0;
	return NULL;
}

int schedi_worker_init(int count)
{
	schedi_flog("schedi_worker_init call (code:requested worker count)",count);
	int ret = 0;

	for (int i = 0; i < count && i < SCHEDI_MAX_WORKERS; i++) {
		schedi_flog("creating thread for worker. (code:worker indice)",i);
		workers[i].id = i;
		workers[i].shutdown = 0;
		workers[i].alive = 1;
		int err = pthread_create(&workers[i].thread, NULL, worker_routine,
		                         &workers[i]);
		if (err) {
			workers[i].alive = 0;
			ret = err;
			schedi_flog("pthread_create failed on schedi_worker_init. (code:worker indice)", i);
			schedi_flog("pthread_create failed on schedi_worker_init. (code:err)", err);
			break;
		}
		worker_count++;
	}
	schedi_flog("schedi_worker_init return",ret);
	return ret;
}

int schedi_worker_initd(void)
{
	schedi_flog("schedi_worker_init with default parameters",0);
	return schedi_worker_init(SCHEDI_DEFAULT_WORKER_COUNT);
}

int schedi_worker_deinit(void)
{
	schedi_flog("schedi_worker_deinit call",0);
	int ret = 0;
#ifndef LOCKLESS_READYJOB
	if(ret = schedi_lock_readyjob_mutex()) {
		schedi_flog("schedi_lock_readyjob_mutex() returned an error. (code:err)", ret);
	}
#endif /* LOCKLESS_READYJOB */
	for (int i = 0; i < worker_count; i++)
		workers[i].shutdown = 1;
	schedi_flog("schedi_worker_deinit, shutdown marked for all workers.",0);
	schedi_flog("Waking up all the workers that are sleeping for job waiting safely.", 0);

#ifndef LOCKLESS_READYJOB
	if(ret = schedi_wake_all_job_waiters_lockless()) {
		schedi_flog("schedi_wake_all_job_waiters() returned an error. (code:err)", ret);
	}
	if(ret = schedi_unlock_readyjob_mutex()) {
		schedi_flog("schedi_unlock_readyjob_mutex() returned an error. (code:err)", ret);
	}
#endif /* LOCKLESS_READYJOB */

	for (int i = 0; i < worker_count; i++) {
		schedi_flog("schedi_worker_deinit, worker joining. (code:worker indice)",i);
		int err = pthread_join(workers[i].thread, NULL);
		if (err) {
			schedi_flog("schedi_worker_deinit, worker couldnt join. (code:err)",err);
			ret = err;
		}
	}

	worker_count = 0;
	schedi_flog("schedi_worker_deinit return", ret);
	return ret;
}
