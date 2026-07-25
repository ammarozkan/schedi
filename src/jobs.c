#include <schedi/jobs.h>
#include <schedi/epoll.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <stdbool.h>
#include <schedi/log.h>

/*
 * Destroy Queue
 *
 * Circular buffer of job pointers flagged for destruction but still
 * locked by a worker.  schedi_job_destroy_queue_check() is called
 * periodically to retry them.
 */

#define SCHEDI_DQ_SIZE 1024

/**
 * buf represents jobs.
 * for every job; state == 0 non-poppable.
 */

static struct {
	struct schedi_job *buf[SCHEDI_DQ_SIZE];
	_Atomic bool ready[SCHEDI_DQ_SIZE];
	_Atomic long long htc;	// contains head (first 16), tail (16), count (high 16). Rest highest 16 is label
							// for preventing ABA problem on really high amount of work in a little time.
							// its really a low chance (65536 push and pop while CAS loop calculating!) but
							// extra protection.
} dq;

_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2, "CPU with lockless atomic long long support would be great.");

#define _BITS16 0b1111111111111111
#define BITS16(x) ((long long)_BITS16<<((x)*16))

struct HTC {
	uint32_t head, tail, count, label;
};

#define PACK_HTC(htc) ((long long)(htc).head | (long long)(htc).tail << 16 | (long long)(htc).count << 32 | (long long)(htc).label << 48)
#define UNPACK_HTC(htc) ((struct HTC){ 	\
	.head = (uint32_t)((htc) >> 0) & BITS16(0), 	\
	.tail = (uint32_t)((htc) >> 16) & BITS16(0), 	\
	.count = (uint32_t)((htc) >> 32) & BITS16(0), 	\
	.label = (uint32_t)((htc) >> 48) & BITS16(0)} 	\
)


// returns -1 if queue is full
// returns 0 in success

#define DEFINE_DQ_PUSH_ATOMIC(name, super) 									\
static int name(struct schedi_job *job) 									\
{ 																			\
	long long htc = atomic_load_explicit(&dq.htc, memory_order_relaxed); 	\
	long long desired; 														\
																			\
	struct HTC _htc, _des; 													\
																			\
																			\
	do { 																	\
		_htc = UNPACK_HTC(htc); 											\
																			\
		if (_htc.count >= SCHEDI_DQ_SIZE) return -1;						\
																			\
		_des = _htc; 														\
		_des.head = (_htc.head + 1) % SCHEDI_DQ_SIZE; 						\
		super; 																\
																			\
		/* doing something unique */										\
		_des.label = (_des.label + 5 + 										\
			((long long)job)%65200 + _des.head%5)%65536; 					\
																			\
		desired = PACK_HTC(_des); 											\
	} while(!atomic_compare_exchange_strong(&dq.htc, &htc, desired));		\
																			\
	dq.buf[_htc.head] = job;												\
	atomic_store_explicit(&dq.ready[_htc.head], true, memory_order_release);\
}


// returns NULL if theres no current pops ready.
// returns 

#define DEFINE_DQ_POP_ATOMIC(name, super)									\
static struct schedi_job* name()											\
{																			\
	long long htc = atomic_load_explicit(&dq.htc, memory_order_relaxed);	\
	long long desired;														\
																			\
	struct HTC _htc, _des;													\
																			\
																			\
	do {																	\
		_htc = UNPACK_HTC(htc);												\
		bool ready = atomic_load_explicit(&dq.ready[_htc.tail], 			\
			memory_order_acquire);											\
																			\
		if (_htc.count == 0 || !ready) return NULL;							\
																			\
		_des = _htc;														\
		_des.tail = (_htc.tail + 1) % SCHEDI_DQ_SIZE;						\
		super;																\
																			\
		/* lets do something unique */										\
		_des.label = (_des.label + 3 + 										\
			((long long)dq.buf[_htc.tail])%65200 + _des.head%5)%65536;		\
																			\
		desired = PACK_HTC(_des);											\
	} while(!atomic_compare_exchange_strong(&dq.htc, &htc, desired));		\
																			\
	struct schedi_job* job = dq.buf[_htc.tail];								\
	atomic_store_explicit(&dq.ready[_htc.tail], false, 						\
		memory_order_release);												\
	return job;																\
}

// defining both super and non super versions of the function here.
DEFINE_DQ_PUSH_ATOMIC(dq_push_atomic, _des.count += 1);
DEFINE_DQ_PUSH_ATOMIC(dq_super_push_atomic, (void)0);

DEFINE_DQ_POP_ATOMIC(dq_pop_atomic, _des.count -= 1);
DEFINE_DQ_POP_ATOMIC(dq_super_pop_atomic, (void)0);


// super pop and push does not do anything to the count
// variable. So the destroy queue can pop a job and push
// it without concerning if someone else placed another
// job while it is calculating it.
static int dq_super_push(struct schedi_job *job)
{
	return dq_super_push_atomic(job);
}

static struct schedi_job *dq_super_pop(void)
{
	return dq_super_pop_atomic();
}

static int dq_push(struct schedi_job *job)
{
	return dq_push_atomic(job);
}

static struct schedi_job *dq_pop(void)
{
	return dq_pop_atomic();
}

/*
 * Static Array Management
 *
 * All jobs live in a fixed-size array (schedi_job_array). Free slots are
 * tracked as sorted, non-overlapping [a, b) sections that get merged
 * when adjacent holes appear.
 */

static struct schedi_job_array job_array;

static void sections_init(void)
{
	job_array.empty_sections[0] = (struct schedi_section){
		.a = 0, .b = SCHEDI_MAXIMUM_JOBS, .active = 1
	};
}

/* Claim the lowest available index, mark unstable so no one touches it, return the job. */
static struct schedi_job *section_claim(void)
{
	pthread_mutex_lock(&job_array.section_lock);

	for (int i = 0; i < SCHEDI_MAXIMUM_EMPTY_SECTIONS; i++) {
		struct schedi_section *s = &job_array.empty_sections[i];
		if (!s->active)
			continue;
		size_t idx = s->a;
		s->a++;
		if (s->a >= s->b)
			s->active = 0;

		struct schedi_job *job = &job_array.jobs[idx];
		job->meta_flag = SCHEDI_JOB_METAFLAG_UNSTABLE;
		pthread_mutex_unlock(&job_array.section_lock);
		return job;
	}

	pthread_mutex_unlock(&job_array.section_lock);
	return NULL;
}

/* Release index idx back to the free pool, merging adjacent sections. */
static void section_release(size_t idx)
{
	pthread_mutex_lock(&job_array.section_lock);
	int merged = 0;

	for (int i = 0; i < SCHEDI_MAXIMUM_EMPTY_SECTIONS; i++) {
		struct schedi_section *s = &job_array.empty_sections[i];
		if (!s->active)
			continue;
		if (idx + 1 == s->a) {
			s->a = idx;
			merged = 1;
			break;
		}
		if (s->b == idx) {
			s->b = idx + 1;
			merged = 1;
			break;
		}
	}

	if (!merged) {
		for (int i = 0; i < SCHEDI_MAXIMUM_EMPTY_SECTIONS; i++) {
			struct schedi_section *s = &job_array.empty_sections[i];
			if (s->active)
				continue;
			s->a = idx;
			s->b = idx + 1;
			s->active = 1;
			pthread_mutex_unlock(&job_array.section_lock);
			return;
		}
		job_array.flags |= 1;
	}
	pthread_mutex_unlock(&job_array.section_lock);
}


/*
 * Main Job System
 *
 */


// When a job becomes "ready", this thing is signaled.
// Before marking a job "ready" lock the mutex. Then mark
// it, then signal this. Then unlock the mutex. When a picker
// starts checking for jobs will lock the readyjob_mutex
// and see if theres any good jobs. Then will wait for
// readyjob_cond with readyjob_mutex unlocking atomically.
// So there will be no unatomicity for between checking the
// jobs and system setting 1 job ready and this thing misses it.
#ifndef LOCKLESS_READYJOB
pthread_mutex_t readyjob_mutex;
pthread_cond_t readyjob_cond;
#endif /*LOCKLESS_READYJOB*/


#ifndef LOCKLESS_READYJOB
void schedi_pickingreadyjob()
{
	schedi_flog("On schedi_pickingreadyjob(), locking readyjob_mutex", 0);
	pthread_mutex_lock(&readyjob_mutex);
	schedi_flog("On schedi_pickingreadyjob(), readyjob_mutex locked", 0);
}

void schedi_waitforjob()
{
	int ret;
	schedi_flog("Calling pthread_cond_wait on readyjob_cond on schedi_waitforjob()",0);
	ret = pthread_cond_wait(&readyjob_cond, &readyjob_mutex);
	schedi_flog("pthread_cond_wait called on readyjob_cond on schedi_waitforjob()",ret);
	schedi_flog("unlocking readyjob_mutex on schedi_waitforjob()",ret);
	pthread_mutex_unlock(&readyjob_mutex); // ISSUE_SHUTDOWN_CHECK_ON_LOCK_WORKER
}

void schedi_pickedreadyjob()
{
	int ret = pthread_mutex_unlock(&readyjob_mutex);
	schedi_flog("schedi_pickedreadyjob() pthread_mutex_unlock(&readyjob_mutex)",ret);
}

int schedi_wake_all_job_waiters()
{
	int ret = 0;
	schedi_flog("On schedi_wake_all_job_waiters(), locking readyjob_mutex", 0);
	ret |= pthread_mutex_lock(&readyjob_mutex);
	schedi_flog("on schedi_wake_all_job_waiters() pthread_mutex_lock(&readyjob_mutex)",ret);
	ret |= pthread_cond_broadcast(&readyjob_cond);
	ret |= pthread_mutex_unlock(&readyjob_mutex);
	schedi_flog("pthread_mutex_unlock(&readyjob_mutex)",ret);
	return ret;
}

int schedi_wake_job_waiter()
{
	int ret = 0;
	schedi_flog("on schedi_wake_job_waiter() pthread_mutex_lock(&readyjob_mutex)",ret);
	ret |= pthread_cond_signal(&readyjob_cond);
	return ret;
}
#else
int schedi_wake_all_job_waiters() { return 0; }
#endif /*LOCKLESS_READYJOB*/


int schedi_job_init(void)
{
	int error = 0, _error;
#ifndef LOCKLESS_READYJOB
	_error = pthread_cond_init(&readyjob_cond, NULL);
	if(_error != 0) {
		error |= _error;
	}
	_error = pthread_mutex_init(&readyjob_mutex, NULL);
	if(_error != 0) {
		error |= _error;
	}
#endif /*LOCKLESS_READYJOB*/
	sections_init();
	_error = pthread_mutex_init(&job_array.section_lock, NULL);
	if(_error != 0) {
		error |= _error;
	}
	return error;
}

int schedi_job_deinit(void)
{
	int error = 0, _error;
#ifndef LOCKLESS_READYJOB
	_error = pthread_cond_destroy(&readyjob_cond);
	if(_error != 0) {
		error |= _error;
	}
	_error = pthread_mutex_destroy(&readyjob_mutex);
	if(_error != 0) {
		error |= _error;
	}
#endif /*LOCKLESS_READYJOB*/

	_error = pthread_mutex_destroy(&job_array.section_lock);
	if(_error != 0) {
		error |= _error;
	}
	return error;
}

// adds 1 to job count
static void schedi_ready_jobs_cache_add_job_count()
{
	atomic_fetch_add_explicit(&job_array.ready_jobs_cache_reorganize_counter, 1, memory_order_release);
	uint64_t _job_count = atomic_load_explicit(&job_array.job_count, memory_order_acquire);
	uint64_t desired = 0;

	do {
		uint32_t peaked_job_count = (_job_count >> 32) & 0xFFFFFFFF;
		uint32_t job_count = (_job_count & 0xFFFFFFFF);

		if((job_count += 1) > peaked_job_count) {
			//schedi_flog("peaked_job_count increasing", (int)job_count);
			peaked_job_count = job_count;
		}

		desired = (uint64_t)job_count | ((uint64_t)peaked_job_count << 32);


	} while(!atomic_compare_exchange_strong_explicit(&job_array.job_count, &_job_count, 
		desired, memory_order_release, memory_order_relaxed));
}

// subs 1 from job count
static void schedi_ready_jobs_cache_sub_job_count()
{
	atomic_fetch_add_explicit(&job_array.ready_jobs_cache_reorganize_counter, 1, memory_order_release);
	uint64_t _job_count = atomic_load_explicit(&job_array.job_count, memory_order_acquire);
	uint64_t desired = 0;

	do {
		uint32_t peaked_job_count = (_job_count >> 32) & 0xFFFFFFFF;
		uint32_t job_count = (_job_count & 0xFFFFFFFF);

		job_count -= 1;

		desired = (uint64_t)job_count | ((uint64_t)peaked_job_count << 32);

	} while(!atomic_compare_exchange_strong_explicit(&job_array.job_count, &_job_count, 
		desired, memory_order_release, memory_order_relaxed));
}

#include <stdio.h>
struct schedi_job* schedi_cache_job_ready_pop()
{
	if((job_array.job_count & 0xFFFFFFFF) == 0) return NULL;
	for(unsigned int i = 0 ; i < SCHEDI_JOBS_CACHE_CHECK ; i += 1) {
		struct schedi_job* checked_cache = atomic_load_explicit(&job_array.ready_jobs_cache[i], memory_order_acquire);
		do {
			// overhead here
			//printf("checked_cache:0x%x\n",checked_cache);
			if(checked_cache == NULL) break;
			// trying to put NULL if its not NULL.
			// on success, I will have a pointer that no one has popped out.
			// if one is popped, by logic of CAS loop, it became NULL so, continue.
			// but what if a job putted prior place while I am checking this?
			// should I go back and restart the loop? Eh. If I couldnt find anything,
			// it will be restarted eventually if thread is not sleeping. If theres a
			// sleep-system, no one should be able to put a new job while I am checking.
			// Am I missing something?
			if(atomic_compare_exchange_strong_explicit( &job_array.ready_jobs_cache[i],
					&checked_cache, NULL, memory_order_release, memory_order_relaxed)) {
				schedi_ready_jobs_cache_sub_job_count();
				return checked_cache; // cache == checked_cache. Success.
			} else {
			schedi_flog("checked_cache check (code:checked_cache>0)", checked_cache>0);

			}
		} while(1);
	}

	return NULL; // not any ready jobs
}

// checks the jobs one by one. maybe optimization for that?
void schedi_cache_job_ready(struct schedi_job* job)
{
	for(unsigned int i = 0 ; i < SCHEDI_MAXIMUM_JOBS ; i += 1) {
		struct schedi_job* checked_cache = atomic_load_explicit(&job_array.ready_jobs_cache[i], memory_order_acquire);
		do {
			if(checked_cache != NULL) break;
			if (atomic_compare_exchange_strong_explicit(&job_array.ready_jobs_cache[i], 
				&checked_cache, job, memory_order_release, memory_order_relaxed)) {
				schedi_ready_jobs_cache_add_job_count();
				return; // its a success. lets go away.
			}
		} while(1);
	}
}

static struct schedi_job* schedi_ready_jobs_cache_pop_specific(unsigned int indice)
{
	struct schedi_job* checked_cache = atomic_load_explicit(&job_array.ready_jobs_cache[indice], memory_order_acquire);
	do {
		if(checked_cache == NULL) break;
		if(atomic_compare_exchange_strong_explicit( &job_array.ready_jobs_cache[indice],
			&checked_cache, NULL, memory_order_release, memory_order_relaxed)) {
				schedi_ready_jobs_cache_sub_job_count();					
			return checked_cache; // cache == checked_cache. Success.
		}
	} while(1);
	return NULL;
}

void schedi_ready_jobs_cache_reorganize()
{
	// only loops from maximum possible elements

	uint32_t peaked_job_count = (job_array.job_count >> 32) & 0xFFFFFFFF;
	uint32_t reorganized_jobs = 0;
	for(unsigned int i = peaked_job_count ; i <= SCHEDI_MAXIMUM_JOBS ; i -= 1) {
		struct schedi_job* job = schedi_ready_jobs_cache_pop_specific(i);
		if (job == NULL) continue;
		schedi_cache_job_ready(job); // popping a job and caching it closer
#ifndef LOCKLESS_READYJOB
		schedi_wake_job_waiter(); // no one will notice its inside of interval unless this
#endif /*LOCKLESS_READYJOB*/
		reorganized_jobs += 1;
		if (i <= reorganized_jobs) { 
			// if 5 jobs are reorganized and we came from end to start and 5 jobs left 
			// (we're at indice 5 so 0,1,2,3,4 is going to be checked) those 5 jobs
			// for sure filled up 0,1,2,3,4 for this call. If someone edits
			// the list while we're checking, it will increase the reorganize_counter
			// anyway so will be aware of that too when it becomes a problem.
			break;
		}
	}
}

// checks if reorganization is required or not, and starts reorganization
void schedi_ready_jobs_cache_reorganize_tick()
{
	int counter = atomic_load_explicit(&job_array.ready_jobs_cache_reorganize_counter, memory_order_acquire);
	int desired = 0;
	do {
		if(counter < SCHEDI_JOBS_CACHE_CHECK) { // no pop-push operation to get jobs beyond check interval
			schedi_flog("cache is safe",0);
			break;
		}

		if(atomic_compare_exchange_strong_explicit(&job_array.ready_jobs_cache_reorganize_counter,
			&counter, desired, memory_order_release, memory_order_relaxed)) {
			// we got the counter. it is zero now and only we did it.
			schedi_flog("cache was not safe. fixing it",counter);
			schedi_ready_jobs_cache_reorganize(); // reorganization is our responsibility.
			break;
		}
	} while(1);
}


// will or-set it if its modifiable. will fail if flag contains one of banned_flags.
// returns 1 on fail, 0 on success.
unsigned int schedi_job_mark_or(struct schedi_job* job, unsigned int flag, unsigned int banned_flags, bool ban_access)
{
	unsigned int meta_flag = atomic_load_explicit(&(job->meta_flag), memory_order_acquire);
	unsigned int desired;

	unsigned int ban;

	while( !(ban = (meta_flag & banned_flags) | ((-(int)ban_access) & (meta_flag & SCHEDI_JOB_METAFLAG_ACCESS))) ) {
		desired = meta_flag | flag;

		if(!atomic_compare_exchange_strong(&(job->meta_flag), &meta_flag, desired))
			return 0;
	}

	return ban;
}

// will remove a set it if its modifiable. will fail if flag contains one of banned_flags.
// returns 1 on fail, 0 on success.
// also fails with returning the unmark target flag if there is no such flag
unsigned int schedi_job_unmark_or(struct schedi_job* job, unsigned int flag, unsigned int banned_flags, bool ban_access)
{
	unsigned int meta_flag = atomic_load_explicit(&(job->meta_flag), memory_order_acquire);
	unsigned int desired;

	unsigned int ban;

	while( !(ban = (meta_flag & banned_flags) | ((-(int)ban_access) & (meta_flag & SCHEDI_JOB_METAFLAG_ACCESS))) 
		&& meta_flag & flag) {
		desired = meta_flag & (~flag);

		if(!atomic_compare_exchange_strong(&(job->meta_flag), &meta_flag, desired))
			return 0;
	}

	return ban;
}

unsigned int schedi_job_mark_or_default(struct schedi_job* job, unsigned int flag)
{
	return schedi_job_mark_or(job, flag, SCHEDI_JOB_METAFLAG_UNSTABLE, false);
}

// if something is setting the job up (destroying, initializing) cannot be alive right now. its at the control
// of editor.
unsigned int schedi_job_mark_alive(struct schedi_job* job)
{
	return schedi_job_mark_or(job, SCHEDI_JOB_METAFLAG_ALIVE, SCHEDI_JOB_METAFLAG_UNSTABLE, false);
}

// cannot execute if it is not stable thus being edited.
unsigned int schedi_job_mark_executing(struct schedi_job* job)
{
	return schedi_job_mark_or(job, SCHEDI_JOB_METAFLAG_EXECUTING, 
		SCHEDI_JOB_METAFLAG_UNSTABLE |
		SCHEDI_JOB_METAFLAG_EXECUTING, false);
}

unsigned int schedi_job_unmark_executing(struct schedi_job* job)
{
	return schedi_job_unmark_or(job, SCHEDI_JOB_METAFLAG_EXECUTING, 
		SCHEDI_JOB_METAFLAG_UNSTABLE, false);
}

// cannot edit, thus make it unstable, if its being accessed or being edited.
// if something else is making this unstable, then this should not touch it.
unsigned int schedi_job_mark_unstable(struct schedi_job* job)
{
	return schedi_job_mark_or(job, SCHEDI_JOB_METAFLAG_UNSTABLE, 
		SCHEDI_JOB_METAFLAG_UNSTABLE | 
		SCHEDI_JOB_METAFLAG_EXECUTING, true);
}

unsigned int schedi_job_mark_wlldestroy(struct schedi_job* job)
{
	return schedi_job_mark_or(job, SCHEDI_JOB_METAFLAG_WLLDESTROY, 
		SCHEDI_JOB_METAFLAG_UNSTABLE, false);
}

unsigned int schedi_job_unmark_wlldestroy(struct schedi_job* job)
{
	return schedi_job_unmark_or(job, SCHEDI_JOB_METAFLAG_WLLDESTROY, 
		SCHEDI_JOB_METAFLAG_UNSTABLE, false);
}


unsigned int schedi_job_mark_access(struct schedi_job* job)
{
	uint32_t meta_flag = atomic_load_explicit(&(job->meta_flag), memory_order_acquire);
	uint32_t desired;
	uint32_t banned_flags = SCHEDI_JOB_METAFLAG_UNSTABLE;

	unsigned int ban;

	while(!(ban = meta_flag & banned_flags)) {
		desired = meta_flag;
		unsigned int accesses = SCHEDI_JOB_METAFLAG_GETACCESS(desired);
		if (accesses == 15) return meta_flag & SCHEDI_JOB_METAFLAG_ACCESS;
		accesses += 1;
		SCHEDI_JOB_METAFLAG_SETACCESS(desired, accesses);

		if(!atomic_compare_exchange_strong(&(job->meta_flag), &meta_flag, desired))
			return 0;
	}

	return ban;
}

unsigned int schedi_job_unmark_access(struct schedi_job* job)
{
	uint32_t meta_flag = atomic_load_explicit(&(job->meta_flag), memory_order_acquire);
	uint32_t desired;
	uint32_t banned_flags = SCHEDI_JOB_METAFLAG_UNSTABLE;

	unsigned int ban;

	while(!(ban = meta_flag & banned_flags)) {
		desired = meta_flag;
		unsigned int accesses = SCHEDI_JOB_METAFLAG_GETACCESS(desired);
		accesses -= 1;
		SCHEDI_JOB_METAFLAG_SETACCESS(desired, accesses);

		if(!atomic_compare_exchange_strong(&(job->meta_flag), &meta_flag, desired))
			return 0;
	}

	return ban;
}

unsigned int schedi_job_mark_ready(struct schedi_job* job)
{
	return schedi_job_mark_or(job, SCHEDI_JOB_METAFLAG_READY, 
		SCHEDI_JOB_METAFLAG_UNSTABLE | 
		SCHEDI_JOB_METAFLAG_WLLDESTROY, false);
}

unsigned int schedi_job_unmark_ready(struct schedi_job* job)
{
	return schedi_job_unmark_or(job, SCHEDI_JOB_METAFLAG_READY, 
		SCHEDI_JOB_METAFLAG_UNSTABLE | 
		SCHEDI_JOB_METAFLAG_WLLDESTROY, false);
}


// tries destroying now. if nothing else playing with the data, will succesfully
// destroy it.
// returns 0 on failure
// returns 1 on success
int schedi_job_destroy_now(struct schedi_job* job)
{

	if(! schedi_job_mark_unstable(job) ) return 0; // if this didnt set it, failed.

	atomic_fetch_add_explicit(&job->gen, 1, memory_order_release);

	job->state = schedi_job_state_destroying;
	section_release(job - job_array.jobs);

	schedi_job_epoll_requests_list_destroy(job->epoll_list);

	if (job->dtor)
		job->dtor(job->context);

	job->state = schedi_job_state_nulljob;
	atomic_store_explicit(&(job->meta_flag), 0, memory_order_release);
	return 1;
}

/*
 * schedi_job_destroy_queue_check - Retry pending destructions.
 *
 * Walk the destroy queue and try to lock each job. If the lock
 * succeeds, the worker has finished and we can tear it down.
 * If the lock fails the job is still busy; we cycle it to the
 * back of the queue and try the rest.
 *
 * Loops as long as at least one job was destroyed in a pass,
 * so that a burst of completions is drained in one call.
 */

void schedi_job_destroy_queue_check(void)
{
	struct schedi_job *job;
	do {
		job = dq_super_pop();
		if (job == NULL) break;
		if (!schedi_job_destroy_now(job)) {
			dq_super_push(job);
		}
	} while(1);
}


int schedi_job_destroy(struct schedi_job *job)
{
	schedi_job_mark_wlldestroy(job);
	if (schedi_job_destroy_now(job)) {
		return 0;
	} else {
		if(dq_push(job) < 0) {
			schedi_job_mark_or_default(job, SCHEDI_JOB_METAFLAG_DESTROYNOTENT);
			return -2;
		}
		return -1;
	}
}


int schedi_job_epoll_request_return(struct schedi_job_epoll_request *req, int error)
{
	struct schedi_job* owner = atomic_load_explicit(&(req->owner), memory_order_consume);
	if (!owner)
		return -1;

	if (schedi_job_mark_access(owner))
		return -1;


	pthread_mutex_lock(&req->list->lock);

	schedi_list_remove(&req->list->list, &req->node);

	req->list->waiting_count--;

	if (error)
		req->list->err_count++;
	else
		req->list->ret_count++;

	if(req->list->waiting_count == 0) {
		schedi_job_setready(owner);
	}

	pthread_mutex_unlock(&req->list->lock);

	schedi_job_unmark_access(owner);

	return 0;
}


int schedi_job_epoll_socket_return(struct schedi_job_epoll_socket* req, int events)
{
	// applies 

	return 1;
}

/**
 * schedi_job_epoll_socket_condition() - Being called by epoll loop when socket
 * is died or condition is hitted.
 */

int schedi_job_epoll_socket_condition(struct schedi_job_epoll_socket *sock, int error)
{
	// future impl.
	return 1;
}

/*
 * schedi_job_epoll_list_add - Push a request onto a job's epoll_list.
 * 
 * Should be called after "accessing" the job.
 *
 * Links @req at the end of @job->epoll_list. Increments total_req
 * and waiting_count.
 */

void schedi_job_epoll_list_add(struct schedi_job *job, struct schedi_job_epoll_request *req)
{
	struct schedi_job_epoll_requests_list *list = job->epoll_list;

	pthread_mutex_lock(&list->lock);

	schedi_list_add(&list->list, &req->node);
	list->total_req++;
	list->waiting_count++;

	pthread_mutex_unlock(&list->lock);
}

/*
 * schedi_job_epoll_list_remove - Remove a request from a job's epoll_list.
 *
 * Should be called after "accessing" the job.
 *
 * Unlinks @req from @job->epoll_list. Decrements waiting_count.
 */

void schedi_job_epoll_list_remove(struct schedi_job *job, struct schedi_job_epoll_request *req)
{
	struct schedi_job_epoll_requests_list *list = job->epoll_list;

	pthread_mutex_lock(&list->lock);

	schedi_list_remove(&list->list, &req->node);

	list->waiting_count--;

	pthread_mutex_unlock(&list->lock);
}


int schedi_job_tool_epoll(struct schedi_job *job, int socketfd, uint32_t events)
{
	schedi_job_unmark_ready(job);
	struct schedi_job_epoll_request *req = malloc(sizeof(*req));
	if (!req)
		return -1;

	req->socketfd = socketfd;
	req->owner = job;
	req->list = job->epoll_list;

	schedi_job_epoll_list_add(job, req);

	struct schedi_epoll_data *data = malloc(sizeof(*data));
	if (!data) {
		schedi_job_epoll_list_remove(job, req);
		free(req);
		return -2;
	}

	data->type = SCHEDI_EPOLL_DATA_JOB;
	data->as.ptr = req;

	if (schedi_epoll_add(socketfd, data, events) < 0) {
		free(data);
		schedi_job_epoll_list_remove(job, req);
		free(req);
		return -3;
	}

	return 0;
}

struct schedi_job_epoll_requests_list *schedi_job_epoll_requests_list_new(void)
{
	struct schedi_job_epoll_requests_list *list = malloc(sizeof(*list));
	if (!list)
		return NULL;

	pthread_mutex_init(&list->lock, NULL);
	schedi_list_init(&list->list);
	list->total_req = 0;
	list->waiting_count = 0;
	list->ret_count = 0;
	list->err_count = 0;
	return list;
}

void schedi_job_epoll_requests_list_destroy(struct schedi_job_epoll_requests_list *list)
{
	pthread_mutex_lock(&list->lock);
	struct schedi_list_node *node = list->list.first;
	while (node) {
		struct schedi_job_epoll_request *req =
			(struct schedi_job_epoll_request *)node;
		atomic_store_explicit(&(req->owner), NULL, memory_order_release);
		node = node->next;
	}
	pthread_mutex_unlock(&list->lock);
	pthread_mutex_destroy(&list->lock);
	free(list);
}

struct schedi_job *schedi_job_create(void *context, schedi_job_fn run,
                                     void (*dtor)(void *context))
{
	struct schedi_job *job = section_claim();
	if (!job)
		return NULL;

	job->phase = 0;
	job->state = schedi_job_state_suspended;
	job->request_from = NULL;
	job->context = context;
	job->run = run;
	job->dtor = dtor;
	job->epoll_list = schedi_job_epoll_requests_list_new();

	atomic_store_explicit(&(job->meta_flag), SCHEDI_JOB_METAFLAG_ALIVE, memory_order_release);

	return job;
}

struct schedi_job *schedi_job_pickready(void)
{
_pop_pick:
	struct schedi_job *job = schedi_cache_job_ready_pop(); // pops a job from there
	if (!job) return NULL;

	unsigned int bans = schedi_job_mark_access(job);
	if(bans & SCHEDI_JOB_METAFLAG_UNSTABLE) {
		// somebody setted the job "unstable", AFTER putting it to the ready cache.
		// this is not cool and shouldnt be done. lets do not bother with the job
		// and pop another one.
		//
		// This check is actually useless and I can define making a job UNSTABLE
		// after caching it to ready jobs or calling schedi_job_setready an undefined
		// behaviour, shouldnt done.
		goto _pop_pick;
	} else if(bans & SCHEDI_JOB_METAFLAG_ACCESS) {
		// maximum accesses reached for this job.
		schedi_cache_job_ready(job); // putting it back
		return NULL; 	// encounter of another maximum access error is possible
						// lets just return NULL and let what the hell the caller
						// wants to do.
	}

	if(bans) {
		// interestingly, something is off and returned an impossible output.
		schedi_ferr("schedi_job_mark_access did an unexpected behaviour. (code:banned flag)",(int)bans);
		return NULL;
	}

	schedi_job_unmark_ready(job);

	return job;
}


int schedi_job_setready(struct schedi_job* job)
{
	// locks this so we can do setting a job ready atomically.
	int ret;
#ifndef LOCKLESS_READYJOB
	pthread_mutex_lock(&readyjob_mutex);
#endif /*LOCKLESS_READYJOB*/
	ret = schedi_job_mark_ready(job);

	schedi_cache_job_ready(job);

#ifndef LOCKLESS_READYJOB
	if(!ret) pthread_cond_signal(&readyjob_cond);
#endif /*LOCKLESS_READYJOB*/

#ifndef LOCKLESS_READYJOB
	ret |= pthread_mutex_unlock(&readyjob_mutex);
#endif /*LOCKLESS_READYJOB*/

	return ret;
}

void schedi_completion_indicator_init(struct schedi_job_completion_indicator* indicator)
{
	pthread_mutex_init(&indicator->completion_signal_mutex, NULL);
	pthread_cond_init(&indicator->completion_signal, NULL);
	indicator->completion = false;
}

void schedi_completion_indicator_destroy(struct schedi_job_completion_indicator* indicator)
{
	pthread_mutex_destroy(&indicator->completion_signal_mutex);
	pthread_cond_destroy(&indicator->completion_signal);
}

void schedi_job_completion_indicate(struct schedi_job* job)
{
	if(!job->completion_indicator) return;

	pthread_mutex_lock(&job->completion_indicator->completion_signal_mutex);
	job->completion_indicator->completion = true;
	pthread_cond_broadcast(&job->completion_indicator->completion_signal);
	pthread_mutex_unlock(&job->completion_indicator->completion_signal_mutex);
}


void schedi_job_completion_wait(struct schedi_job_completion_indicator* indicator)
{


	pthread_mutex_lock(&indicator->completion_signal_mutex);
	if(indicator->completion) {
		goto _end;
	}

	pthread_cond_wait(&indicator->completion_signal, &indicator->completion_signal_mutex);

_end:
	pthread_mutex_unlock(&indicator->completion_signal_mutex);
}