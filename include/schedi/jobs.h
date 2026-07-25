#ifndef SCHEDI_JOBS_H
#define SCHEDI_JOBS_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include <schedi/list.h>
#include <schedi/jobs/jobs_epoll_socket.h>

#define SCHEDI_MAXIMUM_JOBS (256)
#define SCHEDI_MAXIMUM_EMPTY_SECTIONS (64)

// only first SCHEDI_JOBS_CACHE_CHECK of ready jobs cache is checked on quick checks.
#define SCHEDI_JOBS_CACHE_CHECK 16

/**
 * enum schedi_job_state - Represents a state of a job.
 * @schedi_job_state_nulljob: Theres no jobs. The struct is just a placeholder.
 * @schedi_job_state_suspended: The execution is paused and there is not a worker.
 * @schedi_job_state_working: There is a worker, executing the job.
 * @schedi_job_state_destroying: Job is marked to be destroyed soon.
 */
enum schedi_job_state {
	schedi_job_state_nulljob = 0,
	schedi_job_state_suspended = 1,
	schedi_job_state_working = 2,
	schedi_job_state_destroying = 3
};


#define SCHEDI_JOB_METAFLAG_ALIVE		(1<<0)
#define SCHEDI_JOB_METAFLAG_UNSTABLE		(1<<1)
#define SCHEDI_JOB_METAFLAG_EXECUTING		(1<<2)
#define SCHEDI_JOB_METAFLAG_WLLDESTROY		(1<<3)
#define SCHEDI_JOB_METAFLAG_DESTROYNOTENT	(1<<4) // job couldnt pushed to the destroy queue. its full.
#define SCHEDI_JOB_METAFLAG_READY			(1<<5) // ready to execute

// in later writings, READY should be the last thing while EPOLLREQ_READY 
// and EPOLLSOCK_READY are at full control of their own. After they both
// are flagged, READY will be flagged with readyjob_mutex. Then readyjob_cond
// will be signaled. Currently it should be done just by EPOLLREQ_READY
// and wakey wakey should also invoked by it.

#define SCHEDI_JOB_METAFLAG_GETACCESS(x)	(((x)>>28)&0b1111)
#define SCHEDI_JOB_METAFLAG_SETACCESS(x, acc)	x = ((acc)<<28) | ((x)&(~(0b1111<<28)))
#define SCHEDI_JOB_METAFLAG_ACCESS (0b1111<<28) // helps to get only the access part


struct schedi_job;

/**
 * typedef schedi_job_fn - Job entry point.
 * @job: The job being executed.
 *
 * Return: 1 to set job ready immediately after worker has nothing to do with
 * the job anymore. 0 to not do anything. Lower than 0 on error which marks the 
 * job as failed and triggers cleanup.
 */
typedef int (*schedi_job_fn)(struct schedi_job *job);

/**
 * struct schedi_job_epoll_request - One FD the main epoll is watching for a
 * job.
 * @node: Intrusive list node, chained into the owner's epoll_list. Must be
 *        the first field (offset 0) so the unified list functions work.
 * @socketfd: The file descriptor.
 * @owner: Back-pointer to the job that registered this request.
 * @list: Direct pointer to the owner's epoll_requests_list.
 *
 * The main epoll loop stores a pointer to this struct in
 * epoll_event.data.ptr. When epoll_wait returns, it locks the list's mutex,
 * checks @owner: if NULL the request is stale; otherwise it unlinks the
 * request via schedi_list_remove, updates counters, and unlocks.
 */
struct schedi_job_epoll_request {
	struct schedi_list_node node;
	int socketfd;
	_Atomic(void *) owner; uint64_t job_gen;
	struct schedi_job_epoll_requests_list *list;
};

/**
 * struct schedi_job_epoll_requests_list - Tracks epoll requests for one job.
 * @lock: Protects all list operations (add, remove, clear, owner check).
 * @list: Unified intrusive list head (schedi_list_node chain).
 * @total_req: Total requests ever added (monotonic, never decremented).
 * @waiting_count: Requests currently outstanding.
 * @ret_count: Requests returned successfully.
 * @err_count: Requests returned with epoll error.
 *
 * All modifications to the list (add, remove, return) happen under @lock.
 * schedi_job_epoll_request_return locks @lock, checks @owner, unlinks the
 * request, decrements waiting_count, and updates ret_count/err_count.
 * A job is resumable once waiting_count == 0.
 */
struct schedi_job_epoll_requests_list {
	pthread_mutex_t lock;
	struct schedi_list list;
	_Atomic unsigned int total_req;
	_Atomic unsigned int waiting_count;
	_Atomic unsigned int ret_count;
	_Atomic unsigned int err_count;
};




/**
 * struct schedi_job_request - Represents a job request.
 * @node: Intrusive list node, chained into a schedi_job_requests_list.
 * @done: This is set if the requested job is done.
 * @ret: Return value from the job.
 *
 * If a job is set with a request_ptr, it will set the appropriate
 * values to the request in every required change (e.g. job completion).
 *
 * The requests are not freed by anyone. Just the requester. But be cautious
 * about freeing, because if you free, and the job tries to reach it, it will
 * break. If the job is running, DO NOT FREE. Or somehow implement a feature
 * that notifies the job.
 *
 * done has some special values:
 * 0. Not done and working.
 * 1. Done.
 * 8. Job is freed and destructed without returning something.
 */
struct schedi_job_request {
	struct schedi_list_node node;
	_Atomic int done;
	union {
		int i;
		void *ptr;
	} ret;
};

/**
 * struct schedi_job_requests_list - The job requests list.
 * @list: Unified intrusive list head.
 * @waiting_count: The jobs that are waiting to be completed.
 * @ret_count: The jobs that are returned.
 * @err_count: The jobs that are returned error.
 *
 * Add one request to only one request list. No system frees a request
 * other than the requester.
 */
struct schedi_job_requests_list {
	struct schedi_list list;
	unsigned int waiting_count, ret_count, err_count;
};

struct schedi_job_completion_indicator {
	pthread_mutex_t completion_signal_mutex;
	pthread_cond_t completion_signal;
	volatile bool completion;
};

/**
 * struct schedi_job - Represents a job.
 * @phase: The phase currently job is at. Used when the job is suspended.
 * @gen: Generation counter, incremented on each create/destroy cycle.
 * @state: Represents the job's state such as suspended, working.
 * @meta_flag: Slot-lifetime flags. Low bits indicate ALIVE/UNSTABLE/EXECUTING
 *             status. High 4 bits represent the current "access" reference
 *             count. Read/written atomically; no lock needed for normal reads.
 * @epoll_list: Contains all the information for the requested epolls.
 * @request_from: Request ptr. Modify on non-null after job is finished.
 * @completion_indicator: Used for completion indication.
 * @run: Function pointer to the job's entry point. Set when the job is
 *        created, called by a worker when the job is picked up.
 * @context: Job-local state pointer. Allocated by the job function on first
 *           run, persists across suspend/resume cycles.
 * @dtor: Optional destructor for @context. Called when the job is destroyed
 *        so cleanup happens even if the job never resumes.
 *
 * No "changing" or "relying" access should be made to the job without going
 * through the meta_flag protocol (mark_access/mark_unstable etc.). All flag
 * accesses shall be made with schedi_job_mark_alive, 
 * schedi_job_mark_executing, schedi_job_mark_unstable,
 * schedi_job_mark_access, schedi_job_unmark_access.
 *
 * If a job is flagged with SCHEDI_JOB_METAFLAG_WLLDESTROY, it is being torn down.
 * Workers shall not pick up this job, but if a worker is already executing
 * it, it will finish its work.
 *
 * A suspended job can be picked up by a worker when:
 * 1. epoll_list->waiting_count == 0 (all I/O returned).
 * 2. Job is accessible with schedi_job_mark_access()
 */
struct schedi_job {
	int phase;
	_Atomic uint64_t gen;
	_Atomic enum schedi_job_state state;
	_Atomic uint32_t meta_flag;

	struct schedi_job_epoll_requests_list *epoll_list;

	struct schedi_job_request *request_from;

	struct schedi_job_completion_indicator* completion_indicator;

	schedi_job_fn run;
	void *context;
	void (*dtor)(void *context);
};

/**
 * struct schedi_section - A section.
 * @a: start of this section.
 * @b: end of this section.
 * @active: is this section active?
 */
struct schedi_section {
	size_t a, b;
	int active;
};

/**
 * struct schedi_job_array - The jobs array.
 * @jobs: All the jobs array. A job can be empty.
 * @empty_sections: Represents the empty sections.
 * @section_lock: Serialises section claim/release operations.
 * @flags: Atomic flags, used for SCHEDI_JOB_ARRAY_SECTION_WARN.
 *
 * Jobs live in a fixed-size array rather than dynamic heap allocations.
 * Each slot has a stable address for the lifetime of the process — there
 * is no ABA problem on slot identity because addresses are never recycled.
 * flag_lock per slot is pre-initialised once by schedi_job_array_init() and
 * survives across create/destroy cycles.
 *
 * Free slots are tracked via struct schedi_section: sorted, non-overlapping
 * [a, b) intervals that are merged when adjacent holes appear. Claiming
 * a slot is O(#sections) and release is O(#sections + merge). The
 * section_lock serialises concurrent claims and releases.
 *
 * Flags:
 * - SCHEDI_JOB_ARRAY_SECTION_WARN: "empty_section" list is full. Check
 *   directly the jobs array when a section gets unactive and find
 *   a new empty section to add there. If not, good. Unflag this.
 *   If yes, do not unflag this. There could be more. If this flag
 *   really pops more and more, increase the size of empty_sections
 *   array.
 */
struct schedi_job_array {
	struct schedi_job jobs[SCHEDI_MAXIMUM_JOBS];
	struct schedi_section empty_sections[SCHEDI_MAXIMUM_EMPTY_SECTIONS];
	pthread_mutex_t section_lock;
	_Atomic unsigned int flags;


	_Atomic int ready_jobs_cache_reorganize_counter; 
		// everytime someone caches a job or uncaches it, this will increase. On some high value,
		// cache should be reorganized by a full check.
	_Atomic(struct schedi_job*) ready_jobs_cache[SCHEDI_MAXIMUM_JOBS]; // only accessible with readyjob_lock.
	// as it is only accessible by readyjob_lock, it does not actually required to be lock-free.
	// but I want to get rid of that lock. So I will leave this like that. I don't know how. I will think
	// about it.

	// low 32 bits are job counts, high are top, peaked job count
	_Atomic uint64_t job_count; 
};

/**
 * schedi_cache_job_ready_pop() - pops a job from the ready job cache
 * 
 * Return: the job if theres any. NULL if theres not a ready job in
 * first SCHEDI_JOBS_CACHE_CHECK amount of elements.
 */
struct schedi_job* schedi_cache_job_ready_pop();

/**
 * schedi_cache_job_ready() - Caches a job.
 * 
 * Puts the job to the first empty place on the ready_jobs_cache, atomically.
 */
void schedi_cache_job_ready(struct schedi_job* job);

/** 
 * schedi_ready_jobs_cache_reorganize() - Puts all jobs together without a spacing.
 * 
 * When ready_jobs_cache_reorganize_counter becomes enoughly high, this function
 * should be called to put the jobs together. So pickready function can continue
 * functioning with only checking first SCHEDI_JOBS_CACHE_CHECK amount of jobs.
 * 
 * This spends real processing so if this function can be runned as a seperate
 * thread, or a 2 worker setup, will relax better.
 */
void schedi_ready_jobs_cache_reorganize();

/**
 * schedi_ready_jobs_cache_reorganize_tick() - Does reorganization if needed.
 * 
 * Checks if ready_jobs_cache_reorganize_counter passes SCHEDI_JOBS_CACHE_CHECK
 * and resets it if it passes atomically. If this call resets, it also
 * runs a reorganization.
 */

void schedi_ready_jobs_cache_reorganize_tick();

/*
 * About Functions
 *
 * If a function does not contain what it returns and has an int, it will
 * return 0 on success and lower than 0 on failure.
 */


/**
 * schedi_job_epoll_request_return() - Called by the main epoll loop after an
 * event.
 * @req: The request that triggered the event.
 * @error: Non-zero if epoll returned EPOLLERR or EPOLLHUP for this fd.
 *
 * Locks the owning list's mutex, checks if @req->owner is alive, removes
 * the request from the job's epoll_list via schedi_list_remove, decrements
 * waiting_count, and increments ret_count (or err_count when @error is
 * set). If @req->owner is NULL the request is stale and the function does
 * nothing.
 *
 * Return: 0 on success, -1 if owner is destroyed.
 */
int schedi_job_epoll_request_return(struct schedi_job_epoll_request *req, int error);

/**
 * schedi_job_epoll_socket_return() - Called by the main epoll loop after an
 * event.
 * @sock: The socket registeration that triggered the event.
 * @error: Non-zero if epoll returned EPOLLERR or EPOLLHUP for this fd.
 * 
 * Return: 0 on success and if socket is still alive. -1 if socket or the owner is
 * dead. Caller epoll maintainer will clean up it from the epoll list.
 */
int schedi_job_epoll_socket_return(struct schedi_job_epoll_socket* sock, int events);

/**
 * schedi_job_tool_epoll() - Register an fd with epoll for this job.
 * @job: The job that wants to wait.
 * @socketfd: File descriptor to watch.
 * @events: Epoll event mask (EPOLLIN | EPOLLET | ...).
 *
 * Allocates an schedi_job_epoll_request, links it into the job's epoll_list,
 * increments waiting_count, and registers the fd via schedi_epoll_add.
 * When the fd fires, the main loop calls schedi_job_epoll_request_return
 * which updates ret_count/err_count and decrements waiting_count.
 * The job resumes once waiting_count == 0.
 * 
 * Obviously needs "access" or "execution" flag on. And I recommend this being
 * called inside a job execution thus providing only 1 thread at 1 time.
 *
 * Return: 0 on success, lower than zero on error.
 */
int schedi_job_tool_epoll(struct schedi_job *job, int socketfd, uint32_t events);

/**
 * schedi_job_init() - Pre-initialise the array and cond_t.
 *
 * Called once at program startup, before any workers are spawned.
 * Initializes readyjob_cond, initialises section_lock, then 
 * initializes sections.
 *
 * Return: 0 on success, an error number on failure. 
 */
int schedi_job_init(void);

/**
 * schedi_job_deinit() - Destroy all array mutexes.
 *
 * Called once at program shutdown, after all workers have been joined.
 * Destroys section_lock.
 *
 * Return: 0 on success, an error number on failure. 
 */
int schedi_job_deinit(void);


/**
 * schedi_wake_job_waiter() - Wakes one job waiter.
 * 
 * Return: 0 on success, an error number on failure.
 */
int schedi_wake_job_waiter();

/** 
 * schedi_wake_all_job_waiters() - Wakes all job waiters.
 * 
 * Can be used when all workers are marked to shutdown but they're sleeping
 * here. With this, they are waken up and can shutdown now.
 * 
 * NOTE: When LOCKLESS_READYJOB is defined, workers never sleep on
 * readyjob_cond, making this function a no-op (returns 0 immediately).
 * 
 * Return: 0 on success, an error number on failure (always 0 with LOCKLESS_READYJOB).
 */
int schedi_wake_all_job_waiters();


/** 
 * About Marks
 * 
 * Unstable: indicates that its drastically modified by something else. For
 * example a destroy.
 * 
 * alive: It is alive. A real job is inside this job and contains some values.
 * 
 * executing: A worker thread is currently executing this job.
 * 
 * wlldestroy: Destroy call has been called on this job. Stay away so when the
 * destroy queue check called, it could be removed quickly. Or else, it will be
 * pushed to the queue again and will wait.
 * 
 * destroynotent: Destroy couldn't noted to the destroy queue. Queue could be
 * full. A memory leak could've happen here, and it is actually. With this flag,
 * it could be found if wanted. Destroy function also indicates this with return
 * value but just in case, this flag could do its job someday.
 */


/**
 * schedi_job_mark_or() - Atomically or-set a flag if the meta_flag is
 * modifiable.
 * @job: The job's pointer.
 * @flag: The flag to set.
 * @banned_flags: Flags that must not be set for the operation to proceed.
 * @ban_access: If non-zero, also fail when the access count is non-zero.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_mark_or(struct schedi_job *job, unsigned int flag,
unsigned int banned_flags, bool ban_access);

/**
 * schedi_job_mark_or_default() - Marks the flag unless it is unstable.
 * @job: The job's pointer.
 * @flag: The flag to set.
 * 
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_mark_or_default(struct schedi_job* job, unsigned int flag);

/**
 * schedi_job_unmark_or() - Atomically clear a flag if the meta_flag is
 * modifiable.
 * @job: The job's pointer.
 * @flag: The flag to clear.
 * @banned_flags: Flags that must not be set for the operation to proceed.
 * @ban_access: If non-zero, also fail when the access count is non-zero.
 *
 * Also fails with returning the unmark target flag if there is no such flag
 * in the job's meta_flag.
 * 
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_unmark_or(struct schedi_job *job, unsigned int flag,
                         unsigned int banned_flags, bool ban_access);

/**
 * schedi_job_mark_alive() - Mark the job as alive.
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_ALIVE. Fails if UNSTABLE is set.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_mark_alive(struct schedi_job *job);

/**
 * schedi_job_mark_executing() - Mark the job as executing.
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_EXECUTING. Fails if UNSTABLE and
 * EXECUTING is set.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_mark_executing(struct schedi_job *job);

/**
 * schedi_job_unmark_executing() - Unmark the job as executing.
 * @job: The job's pointer.
 *
 * Unsets SCHEDI_JOB_METAFLAG_EXECUTING. Fails if UNSTABLE
 * is set.
 * 
 * Also fails with returning SCHEDI_JOB_METAFLAG_EXECUTING
 * if there is no such flag to remove in the job's meta_flag.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_unmark_executing(struct schedi_job *job);

/**
 * schedi_job_mark_unstable() - Mark the job as unstable (being edited).
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_UNSTABLE. Fails if UNSTABLE or EXECUTING is
 * already set, or if the access count is non-zero.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_mark_unstable(struct schedi_job *job);

/**
 * schedi_job_mark_wlldestroy() - Mark the job as "this will be destroyed soon"
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_WLLDESTROY. Fails if UNSTABLE is already set. This
 * mark is an indication for system to stay away from this and leave this alone
 * so destroyer could do its job quickly when the time comes.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_mark_wlldestroy(struct schedi_job* job);

/**
 * schedi_job_unmark_wlldestroy() - Unmark the "will be destroyed" flag.
 * @job: The job's pointer.
 *
 * Clears SCHEDI_JOB_METAFLAG_WLLDESTROY. Fails if UNSTABLE is already set.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_unmark_wlldestroy(struct schedi_job* job);

/**
 * schedi_job_mark_access() - Increment the access reference count.
 * @job: The job's pointer.
 *
 * Fails if UNSTABLE is set.
 *
 * Return: 0 on success, banned flags if banneds encountered. Access
 * flags if maximum access encountered.
 */
unsigned int schedi_job_mark_access(struct schedi_job *job);

/**
 * schedi_job_unmark_access() - Decrement the access reference count.
 * @job: The job's pointer.
 *
 * Fails if UNSTABLE is set.
 *
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_unmark_access(struct schedi_job *job);

/**
 * schedi_job_mark_ready() - Triggered by the stuff that job is waiting for.
 * @job: The job's pointer.
 * 
 * Fails if UNSTABLE and WLLDESTROY is set.
 * 
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_mark_ready(struct schedi_job* job);

/** 
 * schedi_job_unmark_ready() - Triggered when job is started being executed.
 * @job: The job's pointer.
 * 
 * Fails if UNSTABLE and WLLDESTROY is set.
 * 
 * Return: 0 on success, banned flags if banneds encountered.
 */
unsigned int schedi_job_unmark_ready(struct schedi_job* job);

/**
 * schedi_job_create() - Allocate and initialise a new job slot.
 * @context: Opaque pointer to job-local state (e.g. bump-allocator region).
 *           Passed back to @run on each invocation and to @dtor on teardown.
 * @run: Entry-point function. Called by a worker when the job is picked up.
 *       Receives the job pointer; uses job->phase as a state-machine step.
 *       Return 0 to complete normally (job is destroyed/reused) or call
 *       schedi_job_tool_epoll and return 0 to suspend until I/O arrives.
 * @dtor: Optional destructor for @context. Called by schedi_job_destroy or
 *        destroy_now regardless of whether the job completed or was
 *        torn down mid-flight. May be NULL.
 *
 * Claims a slot from the static array. section_claim() locks the
 * section_lock, finds a free slot, marks it UNSTABLE, and unlocks.
 * After all fields are initialised, SCHEDI_JOB_METAFLAG_ALIVE is set
 * atomically.
 *
 * The returned job has phase = 0 and state = schedi_job_state_suspended.
 *
 * Return: a pointer to the job on success, NULL on failure.
 */
struct schedi_job *schedi_job_create(void *context, schedi_job_fn run,
				     void (*dtor)(void *context));

/**
 * schedi_job_destroy() - Safely handles the job destroyment.
 * @job: The job's pointer to be destroyed.
 *
 * Sets SCHEDI_JOB_METAFLAG_WLLDESTROY, then tries schedi_job_destroy_now(). If that
 * fails (the slot is locked by a worker or accessed), the job is pushed to
 * the destroy queue for later cleanup by schedi_job_destroy_queue_check().
 *
 * Return: 0 on success, -1 if couldn't destroyed now but added to the queue,
 * -2 if queue was full and couldn't added also to the queue.
 */
int schedi_job_destroy(struct schedi_job *job);

/**
 * schedi_job_pickready() - Return a suspended job ready for execution.
 * 
 * if sleeping after founding no job is being executed, schedi_pickingreadyjob()
 * should be called before this function. If there's no job, then
 * schedi_waitforjob() can be called for sleeping. If there's a job returned,
 * schedi_pickedreadyjob() should be called to unlock the mutex.
 *
 * Scans the job pool for a job that:
 * 1. is in state schedi_job_state_suspended,
 * 2. Accessible, not unstable.
 * 3. has epoll_list->waiting_count == 0.
 *
 * If a candidate is found, it is returned directly (the caller should call
 * schedi_job_mark_executing or similar before working on it). It also does
 * not unmark the job access if it found a succesfull job. After marking it
 * "executing", "accessing" should be removed (should be unmarked).
 *
 * Return: a pointer to a ready job, or NULL if none is ready.
 */
struct schedi_job *schedi_job_pickready(void);

// Locks readyjob_cond so the jobs that did get checked wont be able to
// signal while rest of the jobs being checked.
void schedi_pickingreadyjob();

// cond_wait with using readyjob_cond.
void schedi_waitforjob();

// a job is picked succesfully. unlocks the readyjob_cond.
void schedi_pickedreadyjob();

/**
 * schedi_job_setready() - Sets job ready and signals readyjob_cond atomically.
 * 
 * Also caches the job to the array ready_jobs_cache. So theres no turning back
 * from setting a job ready. Turning back is really unefficient to do again and
 * again. Dont do that.
 * 
 * Return: 0 on success, different than 1 on failure.
 */

int schedi_job_setready(struct schedi_job* job);

/**
 * schedi_completion_indicator_init() - Initializes the indicator.
 */

void schedi_completion_indicator_init(struct schedi_job_completion_indicator* indicator);


/**
 * schedi_completion_indicator_destroy() - Destroys the indicator.
 */
void schedi_completion_indicator_destroy(struct schedi_job_completion_indicator* indicator);

/**
 * schedi_job_completion_indicate() - Indicates a job is completed with indicator.
 * @job: job to be indicated.
 * 
 * locks mutex, sets completion, signals, then unlocks.
 */
void schedi_job_completion_indicate(struct schedi_job* job);

/**
 * schedi_job_completion_wait() - Waits for a completion indicator.
 * @indicator: indicator to be waited
 * 
 * Does not consumes any. If its already indicated by a really old job, it will
 * be signed and exited immediately anyway.
 * 
 */
void schedi_job_completion_wait(struct schedi_job_completion_indicator* indicator);



/**
 * schedi_job_epoll_requests_list_new() - Allocate and initialise an epoll
 * list.
 *
 * Allocates a struct schedi_job_epoll_requests_list on the heap, initialises
 * the mutex, zeroes the list head via schedi_list_init, and sets
 * waiting_count, ret_count, err_count to 0.
 *
 * The caller must call schedi_job_epoll_requests_list_destroy() to tear it
 * down.
 *
 * Return: a pointer on success, NULL on failure.
 */
struct schedi_job_epoll_requests_list *schedi_job_epoll_requests_list_new(void);

/**
 * schedi_job_epoll_requests_list_destroy() - De-initialise and free an epoll
 * list.
 * @list: Pointer to a struct schedi_job_epoll_requests_list previously
 *        returned by schedi_job_epoll_requests_list_new().
 *
 * Locks the list, NULLs all request owners via atomic store, destroys
 * the mutex, and calls free() on @list. The caller must not access @list
 * after this call.
 */
void schedi_job_epoll_requests_list_destroy(struct schedi_job_epoll_requests_list *list);

/**
 * schedi_job_epoll_list_add() - Push a request onto a job's epoll_list.
 * @job: The job that owns the list.
 * @req: The request to add.
 *
 * Links @req at the end of @job->epoll_list under the list lock.
 * Increments total_req and waiting_count.
 */
void schedi_job_epoll_list_add(struct schedi_job *job, struct schedi_job_epoll_request *req);

/**
 * schedi_job_epoll_list_remove() - Remove a request from a job's epoll_list.
 * @job: The job that owns the list.
 * @req: The request to remove.
 *
 * Unlinks @req from @job->epoll_list under the list lock.
 * Decrements waiting_count.
 */
void schedi_job_epoll_list_remove(struct schedi_job *job, struct schedi_job_epoll_request *req);

/**
 * schedi_job_destroy_now() - Internal teardown.
 * @job: The job to destroy.
 *
 * Marks the slot UNSTABLE. On success releases the slot back to the free
 * pool, destroys the epoll list, calls the destructor, clears meta_flag,
 * and returns 1. If mark_unstable fails (slot is busy) returns 0.
 *
 * Return: 1 on success, 0 on failure.
 */
int schedi_job_destroy_now(struct schedi_job *job);

/**
 * schedi_job_destroy_queue_check() - Retry pending destructions.
 *
 * Walk the destroy queue and try schedi_job_destroy_now() on each. If it
 * succeeds the job is torn down; if it fails the job is cycled to the back
 * of the queue.
 *
 * Loops as long as at least one job was destroyed in a single pass.
 */
void schedi_job_destroy_queue_check(void);

#endif /* SCHEDI_JOBS_H */
