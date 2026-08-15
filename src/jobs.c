#include <schedi/jobs.h>
#include <schedi/epoll.h>
#include <schedi/log.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <string.h>

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
	_Atomic long long htc;	// contains head (first 16), tail (16), 
				// count (high 16). Rest highest 16 is label
				// for preventing ABA problem on really high 
				// amount of work in a little time.
				// its really a low chance (65536 push and pop 
				// while CAS loop calculating!) but
				// extra protection.
} dq;

_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2, "CPU with lockless atomic long long support would be great.");

static size_t min(size_t a, size_t b)
{
	return a < b ? a : b;
}


#define _BITS16 0b1111111111111111
#define BITS16(x) ((long long)_BITS16<<((x)*16))

#define HTC_HEAD(x) (x >> 48 & (long long)0xFFFF)
#define HTC_TAIL(x) (x >> 32 & (long long)0xFFFF)
#define HTC_COUNT(x) (x >> 16 & (long long)0xFFFF)
#define HTC_LABEL15(x) (x & (long long)0x7FFF)		// for 15 bit label
#define HTC_LABEL(x) (x & (long long)0xFFFF)		// 16 bit label

struct HTC {
	uint32_t head, tail, count, label;
};

#define PACK_HTC(htc) ((long long)(htc).head << 48 | (long long)(htc).tail << 32 | \
		(long long)(htc).count << 16 | (long long)(htc).label << 0)
#define UNPACK_HTC(htc) ((struct HTC){ 	\
	.head = (uint32_t)((htc) >> 48) & BITS16(0), 	\
	.tail = (uint32_t)((htc) >> 32) & BITS16(0), 	\
	.count = (uint32_t)((htc) >> 16) & BITS16(0), 	\
	.label = (uint32_t)((htc) >> 0) & BITS16(0)} 	\
)


static void dq_dec_count()
{
	long long htc = atomic_load_explicit(&dq.htc, memory_order_relaxed);
	long long desired;
	struct HTC _htc, _des;
	do {
		uint16_t count = HTC_COUNT(htc);
		count -= 1;
		desired = (htc&~((long long)0xFFFF<<16)) | (count<<16);
	} while(!atomic_compare_exchange_strong(&dq.htc, &htc, desired));
}

// returns -1 if queue is full
// returns 0 in success

#define DEFINE_DQ_PUSH_ATOMIC(name, super) 					\
static int name(struct schedi_job *job) 					\
{ 										\
	long long htc = atomic_load_explicit(&dq.htc, memory_order_relaxed); 	\
	long long desired; 			\
	struct HTC _htc, _des; 			\
						\
	do { 					\
		_htc.count = HTC_COUNT(htc);	\
						\
		if (_htc.count >= SCHEDI_DQ_SIZE) return -1;				\
		_htc.head = HTC_HEAD(htc);						\
		_htc.tail = HTC_TAIL(htc);						\
		_htc.label = HTC_LABEL(htc);						\
											\
		_des = _htc; 								\
		_des.head = (_htc.head + 1) % SCHEDI_DQ_SIZE; 				\
		super; 									\
											\
		/* doing something unique */						\
		_des.label = (_des.label + 5 + 						\
			((long long)job)%65200 + _des.head%5)%65536; 			\
											\
		desired = PACK_HTC(_des); 						\
	} while(!atomic_compare_exchange_strong(&dq.htc, &htc, desired));		\
											\
	dq.buf[_htc.head] = job;							\
	atomic_store_explicit(&dq.ready[_htc.head], true, memory_order_release);	\
	return 0;									\
}


// returns NULL if theres no current pops ready.
// returns 

#define DEFINE_DQ_POP_ATOMIC(name, super)					\
static struct schedi_job* name()						\
{										\
	long long htc = atomic_load_explicit(&dq.htc, memory_order_relaxed);	\
	long long desired;							\
										\
	struct HTC _htc, _des;							\
										\
										\
	do {									\
		_htc.count = HTC_COUNT(htc);					\
		_htc.tail = HTC_TAIL(htc);					\
										\
		bool ready = atomic_load_explicit(&dq.ready[_htc.tail], 	\
			memory_order_acquire);					\
										\
		if (_htc.count == 0 || !ready) return NULL;			\
										\
		_htc.head = HTC_HEAD(htc);					\
		_htc.label = HTC_LABEL(htc);					\
										\
		if (_htc.tail == _htc.head) return NULL; /* oversell control */	\
		/* for super_pop use */						\
		_des = _htc;							\
		_des.tail = (_htc.tail + 1) % SCHEDI_DQ_SIZE;			\
		super;								\
										\
		/* lets do something unique */						\
		_des.label = (_des.label + 3 + 						\
			((long long)dq.buf[_htc.tail])%65200 + _des.head%5)%65536;	\
											\
		desired = PACK_HTC(_des);						\
	} while(!atomic_compare_exchange_strong(&dq.htc, &htc, desired));		\
											\
	struct schedi_job* job = dq.buf[_htc.tail];					\
	atomic_store_explicit(&dq.ready[_htc.tail], false, 				\
		memory_order_release);							\
	return job;									\
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
		job_array.flags |= SCHEDI_JOB_ARRAY_SECTION_WARN;
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
	pthread_mutex_lock(&readyjob_mutex);
	ret |= pthread_cond_signal(&readyjob_cond);
	pthread_mutex_unlock(&readyjob_mutex);
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

struct schedi_job* schedi_cache_job_ready_pop()
{
	if((job_array.job_count & 0xFFFFFFFF) == 0) return NULL;
	for(unsigned int i = 0 ; i < SCHEDI_JOBS_CACHE_CHECK ; i += 1) {
		struct schedi_job* checked_cache = atomic_load_explicit(&job_array.ready_jobs_cache[i], memory_order_acquire);
		do {
			// overhead here
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
				atomic_fetch_and_explicit(&job_array.flags, ~SCHEDI_JOB_ARRAY_FULL,
						memory_order_relaxed);
				return checked_cache; // cache == checked_cache. Success.
			} else {
			schedi_flog("checked_cache check (code:checked_cache>0)", checked_cache>0);

			}
		} while(1);
	}

	return NULL; // not any ready jobs
}

// checks the jobs one by one. maybe optimization for that?
int schedi_cache_job_ready(struct schedi_job* job)
{
	for(unsigned int i = 0 ; i < SCHEDI_MAXIMUM_JOBS ; i += 1) {
		struct schedi_job* checked_cache = atomic_load_explicit(&job_array.ready_jobs_cache[i], memory_order_acquire);
		do {
			if(checked_cache != NULL) break;
			if (atomic_compare_exchange_strong_explicit(&job_array.ready_jobs_cache[i], 
				&checked_cache, job, memory_order_release, memory_order_relaxed)) {
				schedi_ready_jobs_cache_add_job_count();
				return 0; // its a success. lets go away.
			}
		} while(1);
	}

	atomic_fetch_or_explicit(&job_array.flags, SCHEDI_JOB_ARRAY_FULL,
			memory_order_relaxed);
	return -1;
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
	for(int i = peaked_job_count-1 ; i >= 0 ; i -= 1) {
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

		if(atomic_compare_exchange_strong_explicit(&(job->meta_flag), &meta_flag, desired, 
					memory_order_release, memory_order_relaxed))
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

		if(atomic_compare_exchange_strong_explicit(&(job->meta_flag), &meta_flag, desired, 
					memory_order_release, memory_order_relaxed))
			return 0;
	}

	return ban;
}


static bool schedi_job_meta_flag_check_allready(unsigned int meta_flag)
{
	unsigned int req = SCHEDI_JOB_METAFLAG_READYBASIC |
		SCHEDI_JOB_METAFLAG_READYEPOLLTOOL |
		SCHEDI_JOB_METAFLAG_READYEPOLLSOCK;
	return (meta_flag & req) == req;
}

// CAS-updates job->meta_flag. Applies @sock_delta to the packed
// available_sockets and @wait_delta to the packed waiting_count, or-sets
// @flag, recomputes the derived READYEPOLLSOCK/READYEPOLLTOOL bits from the
// resulting counts, and triggers schedi_job_setready() when the update
// completes the full readiness set (old readiness false, new readiness true).
//
// Fails returning a positive value (the banned flags) when @banned_flags are
// set, or when @ban_access is set while the access count is non-zero. Fails
// returning -1 when a packed count would leave the 4-bit range 0..15.
//
// Return: 0 on success, positive banned flags on ban, -1 on count out of
// range.
static int schedi_job_meta_flag_cas_update(struct schedi_job* job,
		int sock_delta, int wait_delta, unsigned int flag,
		unsigned int banned_flags, bool ban_access)
{
	uint32_t meta_flag = atomic_load_explicit(&(job->meta_flag), memory_order_acquire);
	uint32_t desired;
	int required = job->required_available_sockets;

	unsigned int ban;

	bool pre_ready, ready;

	while( !(ban = (meta_flag & banned_flags) | ((-(int)ban_access) & (meta_flag & SCHEDI_JOB_METAFLAG_ACCESS))) ) {
		int avail = SCHEDI_JOB_METAFLAG_GETAVAIL(meta_flag) + sock_delta;
		int waiting = SCHEDI_JOB_METAFLAG_GETWAIT(meta_flag) + wait_delta;

		if(avail < 0 || avail > 15 || waiting < 0 || waiting > 15)
			return -1;

		desired = meta_flag;
		SCHEDI_JOB_METAFLAG_SETAVAIL(desired, avail);
		SCHEDI_JOB_METAFLAG_SETWAIT(desired, waiting);
		desired |= flag;
		desired &= ~(SCHEDI_JOB_METAFLAG_READYEPOLLSOCK | SCHEDI_JOB_METAFLAG_READYEPOLLTOOL);
		if(avail >= required)
			desired |= SCHEDI_JOB_METAFLAG_READYEPOLLSOCK;
		if(waiting == 0)
			desired |= SCHEDI_JOB_METAFLAG_READYEPOLLTOOL;

		pre_ready = schedi_job_meta_flag_check_allready(meta_flag);
		ready = schedi_job_meta_flag_check_allready(desired);

		if(atomic_compare_exchange_strong_explicit(&(job->meta_flag), &meta_flag, desired, 
					memory_order_release, memory_order_relaxed)) {
			if(!pre_ready && ready) {
				schedi_job_setready(job);
			}
			return 0;
		}
	}

	return ban;
}

unsigned int schedi_job_mark_or_readiness(struct schedi_job* job, 
		unsigned int flag, unsigned int banned_flags, bool ban_access)
{
	int ret = schedi_job_meta_flag_cas_update(job, 0, 0, flag,
			banned_flags, ban_access);
	return ret > 0 ? (unsigned int)ret : 0;
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

		if(atomic_compare_exchange_strong(&(job->meta_flag), &meta_flag, desired))
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

		if(atomic_compare_exchange_strong(&(job->meta_flag), &meta_flag, desired))
			return 0;
	}

	return ban;
}

unsigned int schedi_job_mark_ready(struct schedi_job* job)
{
	return schedi_job_mark_or(job, SCHEDI_JOB_METAFLAG_READY, 
		SCHEDI_JOB_METAFLAG_UNSTABLE | 
		SCHEDI_JOB_METAFLAG_WLLDESTROY |
		SCHEDI_JOB_METAFLAG_READY, false);
}

unsigned int schedi_job_unmark_ready(struct schedi_job* job)
{
	return schedi_job_unmark_or(job, SCHEDI_JOB_METAFLAG_READY, 
		SCHEDI_JOB_METAFLAG_UNSTABLE | 
		SCHEDI_JOB_METAFLAG_WLLDESTROY, false);
}

unsigned int schedi_job_mark_readybasic(struct schedi_job* job)
{
	return schedi_job_mark_or_readiness(job, SCHEDI_JOB_METAFLAG_READYBASIC, 
		SCHEDI_JOB_METAFLAG_UNSTABLE | 
		SCHEDI_JOB_METAFLAG_WLLDESTROY, false);
}

unsigned int schedi_job_unmark_readybasic(struct schedi_job* job)
{
	return schedi_job_unmark_or(job, SCHEDI_JOB_METAFLAG_READYBASIC, 
		SCHEDI_JOB_METAFLAG_UNSTABLE | 
		SCHEDI_JOB_METAFLAG_WLLDESTROY, false);
}


// the derived READYEPOLLTOOL and READYEPOLLSOCK bits are not marked directly
// anymore: they are recomputed from the packed available_sockets and
// waiting_count fields on every meta_flag CAS update (see
// schedi_job_meta_flag_cas_update()).

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
		} else {
			dq_dec_count();
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

	unsigned int bans;
	while ((bans = schedi_job_mark_access(owner)) && !(bans & SCHEDI_JOB_METAFLAG_UNSTABLE))
		;

	if (bans)
		return -1;

	uint64_t gen = atomic_load_explicit(&(owner->gen), memory_order_acquire);
	if (gen != req->job_gen) {
		schedi_job_unmark_access(owner);
		return -1;
	}

	struct schedi_job_epoll_requests_list *list = req->list;

	if (error)
		atomic_fetch_add_explicit(&list->err_count, 1, memory_order_relaxed);
	else
		atomic_fetch_add_explicit(&list->ret_count, 1, memory_order_relaxed);

	// the decrement recomputes the derived READYEPOLLTOOL bit and triggers
	// schedi_job_setready() when it completes the readiness set.
	schedi_job_meta_flag_cas_update(owner, 0, -1, 0, 0, false);

	schedi_job_unmark_access(owner);

	return 0;
}


int schedi_job_tool_epoll(struct schedi_job *job, int socketfd, uint32_t events)
{
	struct schedi_job_epoll_request *req = malloc(sizeof(*req));
	if (!req)
		return -1;

	req->socketfd = socketfd;
	req->owner = job;
	req->job_gen = atomic_load_explicit(&(job->gen), memory_order_acquire);
	req->list = job->epoll_list;

	struct schedi_job_epoll_requests_list *list = req->list;
	atomic_fetch_add_explicit(&list->total_req, 1, memory_order_relaxed);

	// the packed waiting_count field is 4 bits: at most 15 requests can be
	// outstanding for a job. fail rather than let the count wrap.
	if(schedi_job_meta_flag_cas_update(job, 0, 1, 0, 0, false) != 0) {
		free(req);
		return -4;
	}

	struct schedi_epoll_data *data = malloc(sizeof(*data));
	if (!data) {
		schedi_job_meta_flag_cas_update(job, 0, -1, 0, 0, false);
		free(req);
		return -2;
	}

	data->type = SCHEDI_EPOLL_DATA_JOB;
	data->as.ptr = req;

	if (schedi_epoll_add(socketfd, data, events) < 0) {
		schedi_job_meta_flag_cas_update(job, 0, -1, 0, 0, false);
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

	list->total_req = 0;
	list->ret_count = 0;
	list->err_count = 0;
	return list;
}

void schedi_job_epoll_requests_list_destroy(struct schedi_job_epoll_requests_list *list)
{
	free(list);
}

static int schedi_job_epoll_socket_count_from(uint64_t htc)
{
	return (int)(htc & SCHEDI_JOB_EPOLL_SOCKET_COUNT_MASK);
}

static int schedi_job_epoll_socket_avail_from(uint64_t htc)
{
	return (int)((htc & SCHEDI_JOB_EPOLL_SOCKET_AVAIL_MASK)
			>> SCHEDI_JOB_EPOLL_SOCKET_AVAIL_SHIFT);
}

// Updates ready socket count on jobs atomically by the atomic change of
// readiness of sockets. For this to work, readiness exchange on sockets
// should be executed with a CAS loop.
static void schedi_job_epoll_socket_jobready_update(
		struct schedi_job_epoll_socket* _socket,
		uint64_t old_htc, uint64_t new_htc)
{
	bool old_ready = old_htc & SCHEDI_JOB_EPOLL_SOCKET_READY;
	bool new_ready = new_htc & SCHEDI_JOB_EPOLL_SOCKET_READY;

	if(old_ready == new_ready)
		return;

	if(!schedi_job_epollsocket_accessjob(_socket)) return;

	struct schedi_job* job = _socket->job;

	// apply the ready-socket delta to the packed available_sockets. the CAS
	// recomputes the derived READYEPOLLSOCK bit from the resulting count and
	// triggers schedi_job_setready() when it completes the readiness set.
	schedi_job_meta_flag_cas_update(job, new_ready ? 1 : -1, 0, 0, 0, false);

	schedi_job_unmark_access(job);
}

// applies a delta to avail and recomputes write_ready and the complete
// readiness in a single CAS on the packed htc.
static void schedi_job_epoll_socket_avail_add(
		struct schedi_job_epoll_socket* _socket, int delta, bool dead)
{
	uint64_t htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	uint64_t des_htc;
	do {
		int des_avail = schedi_job_epoll_socket_avail_from(htc) + delta;
		bool write_ready = (uint32_t)des_avail >= _socket->write_avail_condition;
		bool read_ready = htc & SCHEDI_JOB_EPOLL_SOCKET_READ_READY;
		bool is_dead = dead || (htc & SCHEDI_JOB_EPOLL_SOCKET_DEAD);
		bool ready = read_ready && write_ready || is_dead;

		des_htc = (htc & ~(SCHEDI_JOB_EPOLL_SOCKET_AVAIL_MASK
					| SCHEDI_JOB_EPOLL_SOCKET_WRITE_READY
					| SCHEDI_JOB_EPOLL_SOCKET_READY))
			| ((uint64_t)des_avail << SCHEDI_JOB_EPOLL_SOCKET_AVAIL_SHIFT)
			| (write_ready ? SCHEDI_JOB_EPOLL_SOCKET_WRITE_READY : 0)
			| (ready ? SCHEDI_JOB_EPOLL_SOCKET_READY : 0)
			| (is_dead ? SCHEDI_JOB_EPOLL_SOCKET_DEAD : 0);
	} while(!atomic_compare_exchange_strong_explicit(&_socket->htc,
				&htc, des_htc,
				memory_order_release, memory_order_relaxed));

	schedi_job_epoll_socket_jobready_update(_socket, htc, des_htc);
}

// when socket dies, socket needs to be marked as ready. as avail and count
// add functions are doing that, in cases that those functions are not called,
// that socket_dead function updates socket as ready, doing the appropriate
// same update (schedi_job_epoll_socket_jobready_update())
static void schedi_job_epoll_socket_dead(
		struct schedi_job_epoll_socket* _socket)
{
	uint64_t htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	uint64_t des_htc;
	do {
		des_htc = htc | SCHEDI_JOB_EPOLL_SOCKET_DEAD
			| SCHEDI_JOB_EPOLL_SOCKET_READY;
	} while(!atomic_compare_exchange_strong_explicit(&_socket->htc,
				&htc, des_htc,
				memory_order_release, memory_order_relaxed));

	schedi_job_epoll_socket_jobready_update(_socket, htc, des_htc);
}

// applies a delta to count and recomputes read_ready and the complete
// readiness in a single CAS on the packed htc.
static void schedi_job_epoll_socket_count_add(
		struct schedi_job_epoll_socket* _socket, int delta, bool dead)
{
	uint64_t htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	uint64_t des_htc;
	do {
		int des_count = schedi_job_epoll_socket_count_from(htc) + delta;
		bool read_ready = (uint32_t)des_count >= _socket->read_count_condition;
		bool write_ready = htc & SCHEDI_JOB_EPOLL_SOCKET_WRITE_READY;
		bool is_dead = dead || (htc & SCHEDI_JOB_EPOLL_SOCKET_DEAD);
		bool ready = read_ready && write_ready || is_dead;

		des_htc = (htc & ~(SCHEDI_JOB_EPOLL_SOCKET_COUNT_MASK
					| SCHEDI_JOB_EPOLL_SOCKET_READ_READY
					| SCHEDI_JOB_EPOLL_SOCKET_READY))
			| ((uint64_t)des_count & SCHEDI_JOB_EPOLL_SOCKET_COUNT_MASK)
			| (read_ready ? SCHEDI_JOB_EPOLL_SOCKET_READ_READY : 0)
			| (ready ? SCHEDI_JOB_EPOLL_SOCKET_READY : 0)
			| (is_dead ? SCHEDI_JOB_EPOLL_SOCKET_DEAD : 0);
	} while(!atomic_compare_exchange_strong_explicit(&_socket->htc,
				&htc, des_htc,
				memory_order_release, memory_order_relaxed));

	schedi_job_epoll_socket_jobready_update(_socket, htc, des_htc);
}






struct schedi_job_epoll_socket*
schedi_job_tool_epollsocket(struct schedi_job* job, int socketfd)
{
	struct schedi_job_epoll_socket* sock;
	sock = (typeof(sock))malloc(sizeof(*sock));

	sock->job = job;
	sock->gen = job->gen;

	sock->fd = socketfd;
	// count = 0, avail = BUFFERSIZE, all readiness bits clear
	uint64_t htc = (uint64_t)SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE
		<< SCHEDI_JOB_EPOLL_SOCKET_AVAIL_SHIFT;

	//atomic_store_explicit(&sock->htc, htc, memory_order_release);
	sock->htc = htc;

	sock->read_htc.head = 0;
	sock->read_htc.tail = 0;

	sock->read_count_condition = 1;

	sock->write_htc.head = 0;
	sock->write_htc.tail = 0;

	sock->write_avail_condition = 1;

	sock->fall = 2;

	// if the initial state already satisfies both readiness conditions the
	// socket starts counting toward the job's available_sockets.
	bool read_ready = (uint32_t)schedi_job_epoll_socket_count_from(htc)
		>= sock->read_count_condition;
	bool write_ready = (uint32_t)schedi_job_epoll_socket_avail_from(htc)
		>= sock->write_avail_condition;
	bool ready = read_ready && write_ready;

	uint64_t des_htc = htc
		| (read_ready ? SCHEDI_JOB_EPOLL_SOCKET_READ_READY : 0)
		| (write_ready ? SCHEDI_JOB_EPOLL_SOCKET_WRITE_READY : 0)
		| (ready ? SCHEDI_JOB_EPOLL_SOCKET_READY : 0);
	atomic_store_explicit(&sock->htc, des_htc, memory_order_release);

	if(ready) {
		schedi_job_meta_flag_cas_update(job, 1, 0, 0, 0, false);
	}

	struct schedi_epoll_data* data;
	data = (typeof(data))malloc(sizeof(*data));
	data->type = SCHEDI_EPOLL_DATA_SOCKET;
	data->as.ptr = (void*)sock;
	sock->data_ptr = data;
	if (schedi_epoll_add(socketfd, data,
				EPOLLIN|EPOLLOUT|
				EPOLLONESHOT)) {
		if(ready)
			schedi_job_meta_flag_cas_update(job, -1, 0, 0, 0, false);
		free(sock);
		return NULL;
	}


	return sock;
}

static int schedi_job_epoll_socket_write_buffer_data(
		struct schedi_job_epoll_socket* _socket, const char* data,
		size_t size)
{
	uint64_t htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	int avail = schedi_job_epoll_socket_avail_from(htc);

	int head = atomic_load_explicit(&_socket->write_htc.head,
			memory_order_acquire);

	int new_head = head;

	size_t total = 0;
	size_t write_size = min(size, avail);

	size_t first_write = min(write_size,
			SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE - head);
	memcpy(_socket->write_buffer + head, data, first_write);
	total += first_write;

	if(head+write_size >= SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE) {
		size_t rest_write = write_size-first_write;
		memcpy(_socket->write_buffer, data+first_write, rest_write);
		total += rest_write;
		new_head = rest_write;
	} else {
		new_head += total;
	}

	atomic_store_explicit(&_socket->write_htc.head, 
			new_head, memory_order_release);


	// here, if the dead flag is stalely false but socket is really dead,
	// it could be marked as not-ready while it really is ready. This could
	// happen
	schedi_job_epoll_socket_avail_add(_socket, -(int)total, false);

	return (int)total;
}

int schedi_job_epoll_socket_write(struct schedi_job_epoll_socket* _socket,
		char* data, size_t size)
{
	uint64_t htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	size_t total = 0;

	// if the write buffer is empty, write directly to the socket and send
	// as much as it can take right now.
	if(schedi_job_epoll_socket_avail_from(htc) == SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE) {
		ssize_t sent = send(_socket->fd, data, size, MSG_NOSIGNAL);
		if(sent > 0) {
			total = (size_t)sent;
			data += (size_t)sent;
			size -= (size_t)sent;
		} else if(sent < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
			// hard error: the socket is dead.
			schedi_job_epoll_socket_dead(_socket);
			return -1;
		}
	}

	// assuming there was no data on buffer here, there are some data that 
	// are not and couldnt wroten. that means socket was ready, while buffer
	// was empty. that means epoll shouldve woked this socket up. 
	// (let me name this case#sww11). If socket didnt woke up, it will.
	
	// rest of it will be written to buffer.
	if(size > 0) {
		// writes to buffer with no condition.
		int ret = schedi_job_epoll_socket_write_buffer_data(_socket, data, size);

		total += ret;
		// epoll_ctl_mod here to wakey wakey. (by case#sww11, socket should be
		// inactive on the epoll list OR epoll WILL wake it up anyway. So
		// for both cases, using mod and making epoll be aware of that socket
		// is substantial.)
		//
		// In other words, moding a socket guarantees a next 
		// epoll_wait() returns the socket if its ready.
		schedi_epoll_mod(_socket->fd, _socket->data_ptr,
				EPOLLIN|EPOLLOUT|EPOLLONESHOT);
	}
	return (int)total;
}

int schedi_job_epoll_socket_read(struct schedi_job_epoll_socket* _socket,
		char* data, size_t size)
{
	uint64_t htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	int count = schedi_job_epoll_socket_count_from(htc);
	int tail = atomic_load_explicit(&_socket->read_htc.tail,
			memory_order_acquire);

	size_t count_size = count;
	size_t read_size = min(count_size, size);

	if(read_size + tail >= SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE) {
		size_t fread = SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE - tail;
		size_t rest_read = read_size - fread;

		memcpy(data, _socket->read_buffer + tail, fread);
		memcpy(data + fread, _socket->read_buffer, rest_read);
		tail = rest_read;
	} else {
		memcpy(data, _socket->read_buffer + tail, read_size);
		tail += read_size;
	}

	schedi_job_epoll_socket_count_add(_socket, -(int)read_size, false);

	atomic_store(&_socket->read_htc.tail, tail);

	if(count == SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE) {
		// buffer was full and socket read return probably couldnt fill
		// new data to buffer. lets activate socket and let it try 
		// read_return again.
		schedi_epoll_mod(_socket->fd, _socket->data_ptr, 
				EPOLLIN|EPOLLOUT|EPOLLONESHOT);
	}

	return read_size;
}

int schedi_job_epoll_socket_write_return(struct schedi_job_epoll_socket* _socket)
{
	int tail;
	uint64_t htc;
	int avail;

	tail = atomic_load_explicit(&_socket->write_htc.tail,
			memory_order_acquire);
	
	htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	avail = schedi_job_epoll_socket_avail_from(htc);

avail_changed:
	size_t sendable = SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE - avail;
	size_t total = 0;

	if(sendable == 0)
		return 0;

	size_t first = min(sendable, SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE - tail);

	ssize_t sent = send(_socket->fd, _socket->write_buffer + tail,
			first, MSG_NOSIGNAL);

	if(sent < 0) {
		if(errno == EAGAIN || errno == EWOULDBLOCK) {
			schedi_epoll_mod(_socket->fd, _socket->data_ptr,
				EPOLLIN|EPOLLOUT|EPOLLONESHOT);
			return 0;
		} else {
			schedi_job_epoll_socket_dead(_socket);
			return -1;
		}
	}

	total = sent;
	tail = (tail + (int)sent) % SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE;

	bool dead = false;

	if((size_t)sent == first && first < sendable) {
		size_t rest_write = sendable - first;
		ssize_t sent_rest = send(_socket->fd,
				_socket->write_buffer,
				rest_write, MSG_NOSIGNAL);
		if(sent_rest > 0) {
			total += sent_rest;
			tail = (int)sent_rest;
		} else if(sent_rest < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
			dead = true;
		}
	}

	schedi_job_epoll_socket_avail_add(_socket, (int)total, dead);
	atomic_store(&_socket->write_htc.tail, tail);
	if(dead)
		return -1;

	htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	avail = schedi_job_epoll_socket_avail_from(htc);
	if (avail != SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE) {
		schedi_epoll_mod(_socket->fd, _socket->data_ptr,
				EPOLLIN|EPOLLOUT|
				EPOLLONESHOT); 	// there are 
						// some bytes filled up
						// wake it up
		// I thought of "goto avail_changed" here but on cases where
		// socket is not ready, there will come a loop. Instead,
		// just waking the epoll registiration back up seems logical.
		//
		// When theres nothing here, schedi_job_epoll_socket_write
		// will wake it up when something comes up.
	}


	return (int)total;
}

int schedi_job_epoll_socket_read_return(struct schedi_job_epoll_socket* _socket)
{
	int head = atomic_load_explicit(&_socket->read_htc.head,
			memory_order_acquire);
	uint64_t htc = atomic_load_explicit(&_socket->htc, memory_order_acquire);
	int count = schedi_job_epoll_socket_count_from(htc);

	size_t free = SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE - count;
	size_t total = 0;

	if(free == 0)
		return 0;

	size_t first = min(free, SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE - head);

	ssize_t got = recv(_socket->fd, _socket->read_buffer + head,
			first, 0);
	if(got == 0) {
		schedi_job_epoll_socket_dead(_socket);
		return -1;
	}
	if(got < 0) {
		if(errno == EAGAIN || errno == EWOULDBLOCK) {
			schedi_epoll_mod(_socket->fd, _socket->data_ptr,
					EPOLLIN|EPOLLOUT|EPOLLONESHOT);
			return 0;
		}
		schedi_job_epoll_socket_dead(_socket);
		return -1;
	}

	total = got;
	head = (head + (int)got) % SCHEDI_JOB_EPOLL_SOCKET_BUFFERSIZE;

	bool dead = false;

	if((size_t)got == first && first < free) {
		size_t rest_read = free - first;
		ssize_t got_rest = recv(_socket->fd,
				_socket->read_buffer,
				rest_read, 0);
		if(got_rest > 0) {
			total += got_rest;
			head = (int)got_rest;
		} else if(got_rest == 0) {
			dead = true;
		} else if(errno != EAGAIN && errno != EWOULDBLOCK) {
			dead = true;
		}
	}

	schedi_job_epoll_socket_count_add(_socket, (int)total, dead);

	atomic_store(&_socket->read_htc.head, head);

	if(dead)
		return -1;

	// oneshot is on. add it up again. should I set every event up there?
	// maybe do some #define somewhere not dup(licate) it.
	schedi_epoll_mod(_socket->fd, _socket->data_ptr, EPOLLIN|EPOLLOUT|
			EPOLLONESHOT);

	return (int)total;
}

int schedi_job_epoll_socket_return(struct schedi_job_epoll_socket* _socket, int events)
{
	bool dead = false;

	if(events & EPOLLIN) {
		if(schedi_job_epoll_socket_read_return(_socket) < 0)
			dead = true;
	}

	if(events & EPOLLOUT) {
		if(schedi_job_epoll_socket_write_return(_socket) < 0)
			dead = true;
	}

	if(events & (EPOLLERR | EPOLLHUP) && !(events & (EPOLLIN | EPOLLOUT))) {
		// when EPOLLIN and EPOLLOUT not triggered and socket died,
		// setting the socket dead and ready as write and read return
		// functions doing is not done. So this function just sets the
		// socket dead+ready for this case only.
		schedi_job_epoll_socket_dead(_socket);
		dead = true;
	}

	if(dead)
		return -1;

	return 0;
}

bool schedi_job_epollsocket_accessjob(struct schedi_job_epoll_socket* _socket)
{
	if(!schedi_job_mark_access(_socket->job)) {
		if(_socket->job->gen == _socket->gen) {
			return true;
		}
		schedi_job_unmark_access(_socket->job);
	}

	return false;
}

int schedi_job_tool_epollsocket_done(struct schedi_job_epoll_socket* _socket)
{
	int p = atomic_fetch_sub(&_socket->fall, 1);
	if(p == 1) {
		// this call made the _socket->fall zero.
		// so let it burn.

		if(schedi_job_epollsocket_accessjob(_socket)) {
			if(_socket->htc & SCHEDI_JOB_EPOLL_SOCKET_READY) {
				// the socket no longer counts toward the packed
				// available_sockets; the derived READYEPOLLSOCK bit is
				// recomputed by the CAS update.
				schedi_job_meta_flag_cas_update(_socket->job, -1, 0, 0, 0, false);
			}
			schedi_job_unmark_access(_socket->job);
		}
		free(_socket);
		return 1;
	}

	return 0;
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

	job->required_available_sockets = 0;

	// the packed counts start at 0: available_sockets (0) already meets the
	// zero required_available_sockets and waiting_count (0) means no
	// outstanding requests, so both derived readiness bits start set.
	atomic_store_explicit(&(job->meta_flag),
		SCHEDI_JOB_METAFLAG_ALIVE |
		SCHEDI_JOB_METAFLAG_READYEPOLLSOCK |
		SCHEDI_JOB_METAFLAG_READYEPOLLTOOL, memory_order_release);

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
	schedi_job_unmark_readybasic(job);

	return job;
}



int schedi_job_setready(struct schedi_job* job)
{
	// locks this so we can do setting a job ready atomically.
	int ret = 0;
#ifndef LOCKLESS_READYJOB
	pthread_mutex_lock(&readyjob_mutex);
#endif /*LOCKLESS_READYJOB*/
	if(schedi_job_mark_ready(job)) ret = -1;
	job->state = schedi_job_state_readywaiting;

	if(!ret) {
		if(schedi_cache_job_ready(job)) {
			schedi_job_unmark_ready(job);
			ret = -2;
		}
		// cache failed. unmarking it to indicate.
		// there will be no way to save this.
	}
	// if something else already marked it ready first theres no need to
	// cache it again as that one will be caching it.
	

#ifndef LOCKLESS_READYJOB
	if(!ret) pthread_cond_signal(&readyjob_cond);
#endif /*LOCKLESS_READYJOB*/

#ifndef LOCKLESS_READYJOB
	if(pthread_mutex_unlock(&readyjob_mutex)) {
		ret -= 10;
	}
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
