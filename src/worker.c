#include <schedi/log.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>

#ifdef LOCKLESS_READYJOB
#ifdef SCHEDULE_NONBUSY
#include <sched.h>
#endif /*SCHEDULE_NONBUSY*/
#endif /*LOCKLESS_READYJOB*/

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
		if (!job) {

			//if(ctx->shutdown) break; 	// second test for ISSUE_WORKER_SHUTDOWN_RACE.

			// ISSUE_WORKER_SHUTDOWN_RACE happens here too.
			// As my testing, with 9/47 probability it misses here again (on 128 thread)
			// Current 100% solution to this that I could think of is to 
			// clear ready job cache so no job could be popped out and wait
			// 1 second. We can partially more guarantee that threads will be
			// hitted that conditional sleep. Then mark shutdown, wake them
			// all up. But this is again a coincidence because 1 second
			// does not really guarantee that the threads will continue working.
			// OS may decide that threads will not be executed inside of that
			// time interval. Even 30 seconds does not guarantee that.
			// (a bit extremism here)

			//schedi_waitforjob(); 	// also unlock readyjob_lock
									// I thought that maybe I could
									// check shutdown and go to picking
									// without an unlock. But maybe 
									// giving a space to other waiters
									// more efficient.
									// When the codebase comes to a
									// state that those optimizations could
									// be tested, change and test them.
									// I will call this ISSUE_SHUTDOWN_CHECK_ON_LOCK_WORKER
									// Is doing appropriate things
									// then picking a job without leaving 
									// the lock, or leaving the lock and 
									// going back from all over more efficient?
									// Actually it seems like obvious that
									// holding the lock while doing all
									// the checks a bit stupid.
			goto _continue;
		} else {
			schedi_flog("worker_routine schedi_pickedreadyjob()",0);
			schedi_pickedreadyjob();
		}
#endif /*LOCKLESS_READYJOB*/

_got_a_job:
		job->state = schedi_job_state_working;
		if(schedi_job_mark_executing(job)) goto _continue; 	// somebody stole the job. ok.
															// stealing is illegal so this
															// check can be removed.
		schedi_job_unmark_access(job);	// holding the access from pickready.
		int ret = job->run(job);
		schedi_job_unmark_executing(job); 	// as we success marking execute,
											// we will success on unmarking.
		if(ret < 0) {
			schedi_flog("schedi_job_fn returned <0. Indicating completion.", 0);
			schedi_job_completion_indicate(job);
			schedi_flog("Triggering destroy.",0);
			ret = schedi_job_destroy(job);
			schedi_flog("schedi_job_destroy called. (code:schedi_job_destroy(job))",ret);
		} else if(ret == 1) { 
			schedi_job_setready(job);
		} else {
			job->state = schedi_job_state_suspended;
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
#endif /*SCHEDULE_NONBUSY*/
#endif /*LOCKLESS_READYJOB*/

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

	for (int i = 0; i < worker_count; i++)
		workers[i].shutdown = 1;
	schedi_flog("schedi_worker_deinit, shutdown marked for all workers.",0);
	schedi_flog("Waking up all the workers that are sleeping for job waiting safely.", 0);

#ifndef LOCKLESS_READYJOB
	if(ret = schedi_wake_all_job_waiters()) {
		schedi_flog("schedi_wake_all_job_waiters() returned an error. (code:err)", ret);
	}
#endif /*LOCKLESS_READYJOB*/

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
