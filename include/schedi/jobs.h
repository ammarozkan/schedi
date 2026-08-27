#ifndef SCHEDI_JOBS_H
#define SCHEDI_JOBS_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include <schedi/list.h>

#define SCHEDI_MAXIMUM_JOBS (256)
#define SCHEDI_MAXIMUM_EMPTY_SECTIONS (64)

// Only the first SCHEDI_JOBS_CACHE_CHECK of the ready jobs cache are checked 
// on quick checks.
#define SCHEDI_JOBS_CACHE_CHECK 16

#define EPOLLSOCKET_DONE_JOBSIDE	0
#define EPOLLSOCKET_DONE_EPOLL 		1

// buffer size for each read and write operation on epollsocket system.
#define SCHEDI_JOB_EPOLLSOCKET_BUFFERSIZE 1024

/**
 * enum schedi_job_state - Represents a state of a job.
 * @schedi_job_state_nulljob: There's no job. The struct is just a placeholder.
 * @schedi_job_state_suspended: The execution is paused and there is no worker.
 * @schedi_job_state_readywaiting: Job is ready and waiting to be executed.
 * @schedi_job_state_working: There is a worker, executing the job.
 * @schedi_job_state_destroying: Job is marked to be destroyed soon.
 */
enum schedi_job_state {
	schedi_job_state_nulljob = 0,
	schedi_job_state_suspended = 1,
	schedi_job_state_readywaiting = 2,
	schedi_job_state_working = 3,
	schedi_job_state_destroying = 4
};


// set when the slot is being set up (right after section_claim()) and when
// it is being torn down (first call in schedi_job_destroy_now()).
#define SCHEDI_JOB_METAFLAG_ALIVE		(1<<0)

#define SCHEDI_JOB_METAFLAG_UNSTABLE		(1<<1)
#define SCHEDI_JOB_METAFLAG_EXECUTING		(1<<2)
#define SCHEDI_JOB_METAFLAG_WLLDESTROY		(1<<3)

// job couldn't be pushed to the destroy queue. It's full.
#define SCHEDI_JOB_METAFLAG_DESTROYNOTENT	(1<<4) 

// ready to execute
#define SCHEDI_JOB_METAFLAG_READY			(1<<5) 

// job function returned, wants to be run again
#define SCHEDI_JOB_METAFLAG_READYBASIC		(1<<6) 

// READYEPOLLTOOL and READYEPOLLSOCK are derived bits. They are recomputed
// from the packed counts below on every meta_flag CAS update, so they are
// always consistent with those counts.
// READYEPOLLTOOL is set exactly when the packed waiting_count (number of
// outstanding epoll requests) is 0.
// READYEPOLLSOCK is set when the packed available_sockets count reaches
// required_available_sockets, cleared when it drops back below.
#define SCHEDI_JOB_METAFLAG_READYEPOLLTOOL	(1<<7)
#define SCHEDI_JOB_METAFLAG_READYEPOLLSOCK	(1<<8)

// READYJOB is set when the packed available_jobs count reaches
// required_available_jobs, cleared when it drops back below.
#define SCHEDI_JOB_METAFLAG_READYJOB		(1<<21)

// The job's available_sockets, waiting_count, and available_jobs live in
// meta_flag as 4-bit fields (values 0..15).
// available_sockets tracks the number of ready epoll sockets for the job.
// waiting_count is capped at 15 outstanding requests per job;
// schedi_job_tool_epoll() fails rather than exceeding this limit.
// available_jobs tracks the number of completed dependent jobs.
#define SCHEDI_JOB_METAFLAG_AVSOCK_SHIFT	9
#define SCHEDI_JOB_METAFLAG_AVSOCK_MASK	(0b1111 << 9)
#define SCHEDI_JOB_METAFLAG_WAIT_SHIFT	13
#define SCHEDI_JOB_METAFLAG_WAIT_MASK	(0b1111 << 13)
#define SCHEDI_JOB_METAFLAG_AVJOBS_SHIFT	17
#define SCHEDI_JOB_METAFLAG_AVJOBS_MASK	(0b1111 << 17)
#define SCHEDI_JOB_METAFLAG_GETAVSOCK(x)	(((x) >> SCHEDI_JOB_METAFLAG_AVSOCK_SHIFT) & 0b1111)
#define SCHEDI_JOB_METAFLAG_SETAVSOCK(x, a)	x = ((a) << SCHEDI_JOB_METAFLAG_AVSOCK_SHIFT) \
		| ((x) & ~(SCHEDI_JOB_METAFLAG_AVSOCK_MASK))
#define SCHEDI_JOB_METAFLAG_GETWAIT(x)	(((x) >> SCHEDI_JOB_METAFLAG_WAIT_SHIFT) & 0b1111)
#define SCHEDI_JOB_METAFLAG_SETWAIT(x, a)	x = ((a) << SCHEDI_JOB_METAFLAG_WAIT_SHIFT) \
		| ((x) & ~(SCHEDI_JOB_METAFLAG_WAIT_MASK))
#define SCHEDI_JOB_METAFLAG_GETAVJOBS(x)	(((x) >> SCHEDI_JOB_METAFLAG_AVJOBS_SHIFT) & 0b1111)
#define SCHEDI_JOB_METAFLAG_SETAVJOBS(x, a)	x = ((a) << SCHEDI_JOB_METAFLAG_AVJOBS_SHIFT) \
		| ((x) & ~(SCHEDI_JOB_METAFLAG_AVJOBS_MASK))

// READY is the "last thing" and should be the only flag the readyjob_mutex
// + readyjob_cond machinery watches. READYBASIC means the job is at rest
// and wants to be run again. It is set by the worker automatically when
// the job function returns 1. Also function could return 0 and make system
// do not set READYBASIC to let user control the behaviour, but this can
// involve a dangerous race on destroyment. The "epoll requests returned" 
// situation is tracked by the packed waiting_count in meta_flag. Every 
// meta_flag change — including marking READYBASIC — recomputes the derived 
// readiness bits and flags READY (signaling readyjob_cond) exactly when the 
// full readiness set is present. Marking READYBASIC alone is enough; nothing
// else has to be called.

#define SCHEDI_JOB_METAFLAG_GETACCESS(x)	(((x)>>28)&0b1111)
#define SCHEDI_JOB_METAFLAG_SETACCESS(x, acc)	x = ((acc)<<28) | ((x)&(~(0b1111<<28)))
#define SCHEDI_JOB_METAFLAG_ACCESS (0b1111<<28) // helps to get only the access part


struct schedi_job;

/**
 * typedef schedi_job_fn - Job entry point.
 * @job: The job being executed.
 *
 * Return: 1 sets READYBASIC, 0 does not. When READYBASIC and all the other
 * READY flags are flagged, job becomes ready. Lower than 0 on error, which 
 * marks the job as completed and triggers cleanup.
 */
typedef int (*schedi_job_fn)(struct schedi_job *job);

/**
 * struct schedi_job_epoll_request - One FD the main epoll is watching for a
 * job.
 * @socketfd: The file descriptor.
 * @owner: Back-pointer to the job slot that registered this request.
 * @job_gen: The owner's generation captured at registration time.
 * @list: Direct pointer to the owner's epoll_requests_list.
 *
 * The main epoll loop stores a pointer to this struct in
 * epoll_event.data.ptr. When epoll_wait returns, the loop calls
 * schedi_job_epoll_request_return. Staleness is detected with the
 * generation: the owner slot (a stable address in the static job array)
 * is marked with access, then @job_gen is compared against the slot's
 * current gen. A mismatch means the slot was torn down and possibly
 * reused — the request is stale and is ignored. The slot's gen is
 * incremented on every destroy, so a recycled slot can never match.
 */
struct schedi_job_epoll_request {
	int socketfd;
	_Atomic(void *) owner;
	uint64_t job_gen;
	struct schedi_job_epoll_requests_list *list;
};

/**
 * struct schedi_job_epoll_requests_list - Tracks epoll request counters for
 * one job.
 * @total_req: Total requests ever added (monotonic, never decremented).
 * @ret_count: Requests returned successfully.
 * @err_count: Requests returned with epoll error.
 *
 * schedi_job_tool_epoll increments total_req and the job's packed
 * waiting_count. schedi_job_epoll_request_return decrements the packed
 * waiting_count and increments ret_count (or err_count on error). A job is
 * resumable once waiting_count == 0.
 *
 * waiting_count == 0 indicates the "epoll requests ready" 
 * (SCHEDI_JOB_METAFLAG_READYEPOLLTOOL) situation. The count itself lives in 
 * the job's meta_flag (SCHEDI_JOB_METAFLAG_WAIT_*) as a 4-bit field capped 
 * at 15 outstanding requests per job, and the derived READYEPOLLTOOL bit is 
 * recomputed from it on every meta_flag CAS update. Setting the job READY 
 * must not be done from here directly — the readiness check goes through 
 * the meta_flag CAS update, which flags READY via schedi_job_setready() when 
 * the full readiness set (READYBASIC, READYEPOLLTOOL, READYEPOLLSOCK, READYJOB) becomes 
 * satisfied.
 */
struct schedi_job_epoll_requests_list {
	_Atomic unsigned int total_req;
	_Atomic unsigned int ret_count;
	_Atomic unsigned int err_count;
};

// Bit layout of the packed @htc field, see struct schedi_job_epollsocket.
#define SCHEDI_JOB_EPOLLSOCKET_COUNT_SHIFT 0

#ifdef SCHEDI_EXT_TIMEOUT

#define SCHEDI_JOB_EPOLLSOCKET_AVAIL_SHIFT 28
#define SCHEDI_JOB_EPOLLSOCKET_COUNT_MASK ((1ULL << 28) - 1)
#define SCHEDI_JOB_EPOLLSOCKET_AVAIL_MASK  (((1ULL << 28) - 1) << 28)
#define SCHEDI_JOB_EPOLLSOCKET_TIMEOUT	(1ULL << 57)

#else

#define SCHEDI_JOB_EPOLLSOCKET_AVAIL_SHIFT 29
#define SCHEDI_JOB_EPOLLSOCKET_COUNT_MASK  ((1ULL << 29) - 1)
#define SCHEDI_JOB_EPOLLSOCKET_AVAIL_MASK  (((1ULL << 29) - 1) << 29)

#endif /*SCHEDI_EXT_TIMEOUT*/


#define SCHEDI_JOB_EPOLLSOCKET_EPOLLDID	(1ULL << 58)
// "epoll did". socket is ready no matter what
#define SCHEDI_JOB_EPOLLSOCKET_EPOLLNT		(1ULL << 59)
// jobside is done but epoll side didn't. job is not ready if this
// is set.

#define SCHEDI_JOB_EPOLLSOCKET_READ_READY  (1ULL << 60)
#define SCHEDI_JOB_EPOLLSOCKET_WRITE_READY (1ULL << 61)
#define SCHEDI_JOB_EPOLLSOCKET_READY       (1ULL << 62)
#define SCHEDI_JOB_EPOLLSOCKET_DEAD        (1ULL << 63)

/**
 * struct schedi_job_epollsocket - A long-term epoll socket with a read and a
 * write ring buffer and their packed count/avail/readiness state.
 * @job: The job that owns this socket; it is refreshed for epoll socket
 *       readiness whenever the socket's readiness changes.
 * @gen: The generation of @job at the time the socket was created. Used to
 *       detect that @job was handed over to another job after the socket was
 *       registered.
 * @fd: The file descriptor the socket receives from and sends to.
 * @htc: A single 64-bit word packing the state of both ring buffers, the
 *       dead flag and the readiness flags, so that a count or avail change,
 *       a death and the readiness it results in are committed together in one
 *       atomic CAS. The lowest 29 bits hold count, the next 29 bits hold
 *       avail, then one bit each for epolldid, epollnt, read_ready, 
 *       write_ready, the complete readiness and dead (top bit). Readiness is 
 *       recomputed on every count/avail CAS; a set dead bit forces readiness.
 * @read_htc: Head and tail positions of the read ring buffer.
 * @read_buffer: The read ring buffer. Data received from the socket is stored
 *               here by the epoll system (read_return) until it is consumed.
 * @read_count_condition: The read_ready threshold: read_ready is set once
 *                        count reaches this value. It cannot be above
 *                        SCHEDI_JOB_EPOLLSOCKET_BUFFERSIZE; if it is, the
 *                        behaviour is undefined.
 * @write_htc: Head and tail positions of the write ring buffer.
 * @write_buffer: The write ring buffer. Data to be sent to the socket is
 *                buffered here until it is flushed by the epoll system
 *                (write_return).
 * @write_avail_condition: The write_ready threshold: write_ready is set once
 *                         avail reaches this value.
 * @fall: Reference count for this socket, 2 on start: one reference held by
 *        the owning job, one by epoll registration When the epoll system 
 *        removes the socket from an epoll list (epoll_ctl DEL), it decreases 
 *        this; when the job decides it is done with the socket, it decreases 
 *        this too. The first one that decreases it from 1 frees the socket.
 * @data_ptr: Registered epoll data pointer for all epoll categories.
 * @epolling: Currently adding stuff to epoll. While this is true, epoll
 * should not delete any fds from the epoll list.
 *
 *
 * count is the number of bytes buffered in @read_buffer, ranging from 0 to
 * SCHEDI_JOB_EPOLLSOCKET_BUFFERSIZE. It grows as data is received into the
 * read buffer and shrinks as it is consumed.
 *
 * avail is the number of free bytes in @write_buffer, ranging from 0 to
 * SCHEDI_JOB_EPOLLSOCKET_BUFFERSIZE. It shrinks as data is buffered for
 * writing and grows as it is flushed to the socket.
 *
 * read_ready is set when count >= read_count_condition, write_ready is set
 * when avail >= write_avail_condition, and the complete readiness is their
 * logical AND. If EPOLLDID is set, readiness is true no matter what. If
 * EPOLLNT is set but EPOLLDID is not, readiness is false no matter what.
 * This is for when job is done with the socket (with _done call), readiness 
 * of that socket could be waited for knowing it is fully freed.
 */
struct schedi_job_epollsocket {
	struct schedi_job* job; uint64_t gen;
	int fd;
	_Atomic uint64_t htc;
	struct {
		_Atomic(int) head, tail;
	} read_htc;


	char read_buffer[SCHEDI_JOB_EPOLLSOCKET_BUFFERSIZE];
	uint32_t read_count_condition;
	
	struct {
		_Atomic(int) head, tail;
	} write_htc;

	char write_buffer[SCHEDI_JOB_EPOLLSOCKET_BUFFERSIZE];
	uint32_t write_avail_condition;

	_Atomic int fall;

	struct schedi_epoll_data* data_ptr;

#ifdef SCHEDI_EXT_TIMEOUT
	time_t timeout_sec;
	int to_timer_fd;
#endif /*SCHEDI_EXT_TIMEOUT*/
};



/**
 * struct schedi_job_completion_indicator - Completion indicator structure.
 * @completion_signal_mutex: Mutex to use with the pthread_cond_t.
 * @completion_signal: Signal that will be signaled when job is done.
 * @completion: Variable that will be set to true on completion by worker.
 */
struct schedi_job_completion_indicator {
	pthread_mutex_t completion_signal_mutex;
	pthread_cond_t completion_signal;
	volatile bool completion;
};

/**
 * struct schedi_job - Represents a job.
 * @phase: The phase currently job is at. Used when the job is suspended.
 * @gen: Generation counter, incremented on each create/destroy cycle.
 * @state: Represents the job's state such as suspended, working. While state
 * having destroying, readywaiting, working, suspended; these do not represent
 * the state atomically and should not be depended. @state is only here for
 * previewing the state and not for behaviour reasons.
 * @meta_flag: Slot-lifetime flags. Low bits indicate ALIVE/UNSTABLE/EXECUTING
 *             status. High 4 bits represent the current "access" reference
 *             count. Read/written atomically; no lock needed for normal reads.
 * @epoll_list: Contains all the information for the requested epolls.
 * @requested_job: The job that depends on this one (set by schedi_job_tool_job).
 * @requested_job_gen: Generation of requested_job captured at dependency setup.
 * @required_available_jobs: Threshold for the packed available_jobs count.
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
 * If a job is flagged with SCHEDI_JOB_METAFLAG_WLLDESTROY, it is being torn 
 * down. Workers shall not pick up this job, but if a worker is already 
 * executing it, it will finish its work.
 */
struct schedi_job {
	int phase;
	_Atomic uint64_t gen;
	_Atomic enum schedi_job_state state;
	_Atomic uint32_t meta_flag;

	struct schedi_job_epoll_requests_list epoll_list;

	// ready sockets are counted individually; if the ready socket count
	// reaches required_available_sockets, the derived READYEPOLLSOCK bit is
	// set. On start, that requirement is zero and the bit is set by default
	// (see schedi_job_create()). Use of epoll sockets will update this
	// depending on the ready socket count, and increasing that requirement
	// manually is required.
	int required_available_sockets;

	// the ready socket count (available_sockets) and the epoll requests
	// waiting_count are packed into meta_flag as 4-bit fields, see
	// SCHEDI_JOB_METAFLAG_AVSOCK_* and SCHEDI_JOB_METAFLAG_WAIT_*.
	
	struct schedi_job* requested_job; uint64_t requested_job_gen;
	int required_available_jobs;

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

// Flags used in struct schedi_job_array.flags.
#define SCHEDI_JOB_ARRAY_SECTION_WARN 1
#define SCHEDI_JOB_ARRAY_FULL (1<<2)

/**
 * struct schedi_job_array - The jobs array.
 * @jobs: All the jobs array. A job can be empty.
 * @empty_sections: Represents the empty sections.
 * @section_lock: Serialises section claim/release operations.
 * @flags: Atomic flags, used for SCHEDI_JOB_ARRAY_SECTION_WARN and
 *         SCHEDI_JOB_ARRAY_FULL.
 *
 * Jobs live in a fixed-size array rather than dynamic heap allocations.
 * Each slot has a stable address for the lifetime of the process. ABA problem
 * is solved by the "gen" contained on the job.
 *
 * Free slots are tracked via struct schedi_section: sorted, non-overlapping
 * [a, b) intervals that are merged when adjacent holes appear. Claiming
 * a slot is O(#sections) and release is O(#sections + merge). The
 * section_lock serialises concurrent claims and releases.
 *
 * Flags:
 * - SCHEDI_JOB_ARRAY_SECTION_WARN: "empty_section" list is full. Check
 *   directly the jobs array when a section gets unactive and find
 *   a new empty section to add there. If not, good, unflag this.
 *   If yes, do not unflag this. There could be more. If this flag
 *   really pops more and more, increase the size of empty_sections
 *   array.
 * - SCHEDI_JOB_ARRAY_FULL: the ready_jobs_cache[] array is full. Set when a 
 *   job could not be cached because the ready job cache came out full, cleared 
 *   when a job is popped out of it.
 */
struct schedi_job_array {
	struct schedi_job jobs[SCHEDI_MAXIMUM_JOBS];
	struct schedi_section empty_sections[SCHEDI_MAXIMUM_EMPTY_SECTIONS];
	pthread_mutex_t section_lock;
	
	_Atomic unsigned int flags;

	_Atomic int ready_jobs_cache_reorganize_counter; 
		// every time someone caches a job or uncaches it, this will 
		// increase. On some high value, cache should be reorganized 
		// by a full check.
	_Atomic(struct schedi_job*) ready_jobs_cache[SCHEDI_MAXIMUM_JOBS]; 
	
	// low 32 bits are job counts, high are top, peaked job count
	_Atomic uint64_t job_count; 
};

/**
 * schedi_cache_job_ready_pop() - Pops a job from the ready job cache.
 * 
 * Return: the job, if there's any. NULL if there's not a ready job in the
 * first SCHEDI_JOBS_CACHE_CHECK amount of elements.
 */
struct schedi_job* schedi_cache_job_ready_pop();

/**
 * schedi_cache_job_ready() - Caches a job.
 * 
 * Puts the job to the first empty place on the ready_jobs_cache, atomically.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_cache_job_ready(struct schedi_job* job);

/** 
 * schedi_ready_jobs_cache_reorganize() - Puts all jobs together without a gap.
 * 
 * When ready_jobs_cache_reorganize_counter becomes high enough, this function
 * should be called to put the jobs together. So pickready function can continue
 * functioning with only checking first SCHEDI_JOBS_CACHE_CHECK amount of jobs.
 * 
 * This spends real processing so if this function can be run as a separate 
 * thread, or on a 2 worker setup, would be better.
 */
void schedi_ready_jobs_cache_reorganize();

/**
 * schedi_ready_jobs_cache_reorganize_tick() - Does reorganization if needed.
 * 
 * Checks if ready_jobs_cache_reorganize_counter hitted minimum 
 * SCHEDI_JOBS_CACHE_CHECK and resets it if it passes atomically. If this call 
 * resets, it also runs a reorganization.
 */

void schedi_ready_jobs_cache_reorganize_tick();

/**
 * schedi_job_required_available_socket() - Updates required available socket
 * count and refreshes readiness.
 */
void schedi_job_required_available_socket(struct schedi_job* job, 
		int required_available_sockets);




/**
 * schedi_job_epoll_request_return() - Called by the main epoll loop after an
 * event.
 * @req: The request that triggered the event.
 * @error: Non-zero if epoll returned EPOLLERR or EPOLLHUP for this fd.
 *
 * Marks the request's owner slot with access (bails immediately if that
 * fails, e.g. the slot is UNSTABLE), then compares @req->job_gen against
 * the slot's current gen. If they differ the slot was torn down and
 * possibly reused — the request is stale and this does nothing. Otherwise
 * it decrements the packed waiting_count field and increments ret_count (or
 * err_count when @error is set). If waiting_count reaches 0, the CAS call 
 * calls schedi_job_setready(owner).
 *
 * Return: 0 on success, -1 if the request is stale or the owner is busy.
 */
int schedi_job_epoll_request_return(struct schedi_job_epoll_request *req, int error);

/**
 * schedi_job_tool_epoll() - Register an fd with epoll for this job.
 * @job: The job that wants to wait.
 * @socketfd: File descriptor to watch.
 * @events: Epoll event mask (EPOLLIN | EPOLLET | ...).
 *
 * Allocates a schedi_job_epoll_request, captures the job's generation into
 * req->job_gen, increments total_req and the packed waiting_count field, and
 * registers the fd via schedi_epoll_add. When the fd fires, the main loop
 * calls schedi_job_epoll_request_return which updates ret_count/err_count and
 * decrements the packed waiting_count. The job resumes once waiting_count == 0
 * (and READYBASIC is set).
 * 
 * Obviously needs "access" or "execution" flag on. Other than this will raise
 * some undefined behaviour due to races etc. And I recommend this being called
 * inside a job execution thus providing single-thread only.
 *
 * Return: 0 on success, lower than zero on error.
 */
int schedi_job_tool_epoll(struct schedi_job *job, int socketfd, uint32_t events);


/**
 * schedi_job_epollsocket_push_epoll() - Pushes a socket's fd to the epoll
 * mechanism.
 * @_socket: The socket.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_job_epollsocket_push_epoll(struct schedi_job_epollsocket* _socket);

/**
 * schedi_job_epollsocket_epollisize() - Puts every required fd to epoll
 * mechanism appropriately.
 * @_socket: The socket.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_job_epollsocket_epollisize(struct schedi_job_epollsocket* _socket);

/**
 * schedi_job_epollsocket_epollagain() - Puts the socket back to epoll sstem
 * after epoll is done with it.
 * @_socket: The socket.
 *
 * Does not works if EPOLLDID is not marked, thus returning failure.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_job_epollsocket_epollagain(struct schedi_job_epollsocket *_socket);


#ifndef SCHEDI_EXT_TIMEOUT

/**
 * schedi_job_tool_epollsocket() - Assigns a socket as schedi epoll socket.
 * @job: Job that uses tool epollsocket.
 * @socketfd: Socket to use on entry.
 * @read_cond: Minimum read buffer to make this socket ready.
 * @write_cond: Minimum free buffer space to make this socket ready.
 * 
 * Caller should call this function while job is guaranteed to be in the same 
 * gen, not in an unreliable state (not METAFLAG_UNSTABLE) so this call 
 * requires a schedi_job_mark_access on the job.
 *
 * Return: Returns the associated socket on success, NULL on failure. On
 * exit of job, schedi_job_epollsocket_done() should be called on this to
 * indicate that job does not need that socket anymore.
 */
struct schedi_job_epollsocket* 
schedi_job_tool_epollsocket(struct schedi_job *job, int socketfd, 
		uint32_t read_cond, uint32_t write_cond);
#else

struct schedi_job_epollsocket* 
schedi_job_tool_epollsocket(struct schedi_job *job, int socketfd, 
		uint32_t read_cond, uint32_t write_cond, time_t timeout_sec);

/**
 * schedi_job_epollsocket_timer_push_epoll() - Pushes timer to the epoll
 * mechanism.
 * @_socket: The socket.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_job_epollsocket_timer_push_epoll(
		struct schedi_job_epollsocket *_socket);

/**
 * schedi_job_epollsocket_timeout_cas() - Marks socket as timed out.
 * @_socket: The socket.
 */
void schedi_job_epollsocket_timeout_cas(
		struct schedi_job_epollsocket* _socket);

/**
 * schedi_job_epollsocket_timeout_reset() - Resets the timeout timer.
 * @_socket: epollsocket that will the timer's be resetted.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_job_epollsocket_timeout_reset(
		struct schedi_job_epollsocket* _socket);

/**
 * schedi_job_epollsocket_timeout_stop() - Stops the timeout timer.
 * @_socket: epollsocket that will the timer's be resetted.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_job_epollsocket_timeout_stop(
		struct schedi_job_epollsocket* _socket);


/**
 * schedi_job_epollsocket_timeout() - Informs if socket is timed out or not.
 *
 * Return: Non-zero if timed out, zero if not.
 */
int schedi_job_epollsocket_timeout(struct schedi_job_epollsocket* _socket);

#endif /*SCHEDI_EXT_TIMEOUT*/

/**
 * schedi_job_epollsocket_ready() - Informs if socket is ready or not.
 *
 * Return: non-zero if ready, zero if not on the current check.
 */
int schedi_job_epollsocket_ready(struct schedi_job_epollsocket* _socket);

/**
 * schedi_job_tool_epollsocket_read_cond() - Change reading condition.
 *
 * Changes the condition and updates readiness.
 */
void schedi_job_tool_epollsocket_read_cond(
		struct schedi_job_epollsocket* _socket, uint32_t read_cond);

/**
 * schedi_job_tool_epollsocket_write_cond() - Change writing condition.
 *
 * Changes the condition and updates readiness.
 */
void schedi_job_tool_epollsocket_write_cond(struct schedi_job_epollsocket* _socket,
		uint32_t write_cond);

/**
 * schedi_job_epollsocket_accessjob() - Accesses job if generation is the same.
 *
 * After this call and playing with the job, schedi_job_unmark_access should be
 * called on the job.
 *
 * Return: true if job is accessed successfully. false if not.
 */
bool schedi_job_epollsocket_accessjob(struct schedi_job_epollsocket* sock);



/**
 * schedi_job_epollsocket_done_() - Indicates epollsocket usage is over.
 * @epollside: Tells the function whether this call is made from the epoll
 * system or not. 0 means its the job side, 1 means its epollside's ready
 * socket, 2 means its epollside's write socket.
 *
 * This function is called from epoll system and jobs itself. The last one
 * calling this function will be freeing the socket.
 *
 * Using fetch_sub == 1 check.
 *
 * Return: -1 if socket is not freed yet, 0 if it is freed on this call.
 */
int schedi_job_epollsocket_done_(struct schedi_job_epollsocket*, 
		int epollside);

/**
 * schedi_job_epollsocket_done() - Indicates epollsocket usage is over.
 *
 * Simpler version of the schedi_job_epollsocket_done_ function to use
 * on job functions.
 *
 * Return: -1 if socket is not freed yet, 0 if it is freed on this call.
 */
int schedi_job_epollsocket_done(struct schedi_job_epollsocket*);


/**
 * schedi_job_tool_job() - Establish a job-to-job dependency.
 * @user_job: The job that depends on @target_job completing.
 * @target_job: The job whose completion will advance @user_job's readiness.
 *
 * Sets target_job->requested_job = user_job. When @target_job completes
 * (returns < 0), the worker will increment @user_job's packed available_jobs
 * count, advancing the READYJOB derived bit. Single-dependency: only one
 * user job may depend on a given target. Calling this twice on the same
 * target overwrites the previous dependency.
 *
 * Call schedi_job_required_available_jobs() on @user_job to set the threshold.
 */
void schedi_job_tool_job(struct schedi_job* user_job, struct schedi_job* target_job);

/**
 * schedi_job_required_available_jobs() - Set the job dependency threshold.
 * @job: The job whose READYJOB threshold is being updated.
 * @count: Number of dependent jobs that must complete before READYJOB is set.
 *
 * A count of 0 (the default) means READYJOB is always satisfied.
 */
void schedi_job_required_available_jobs(struct schedi_job* job, int count);

/**
 * schedi_job_add_available_job() - Increment a job's packed available_jobs.
 * @job: The job to advance.
 *
 * Called by the worker when a dependent job completes. If the job's
 * requested_job is stale or inaccessible, this is not called.
 *
 * Return: 0 on success, -1 on count out of range.
 */
int schedi_job_add_available_job(struct schedi_job* job);


/**
 * schedi_job_epollsocket_write() - Buffer data to be sent on the socket.
 * @socket: The epoll socket to buffer into.
 * @data: Data to buffer.
 * @size: Number of bytes to buffer from @data.
 *
 * If there's no data inside of @data, tries sending directly via socket.
 * If couldn't (could be sent some), writes to buffer as much as possible
 * of the remaining data. Then schedi_job_epollsocket_write_return() makes 
 * epoll flush as much as possible when socket is ready.
 *
 * Must only be called from the thread that creates the epoll socket, i.e. the
 * job that owns it. Not thread-safe.
 *
 * Return: Number of bytes buffered, at most @size.
 */
int schedi_job_epollsocket_write(struct schedi_job_epollsocket* socket, 
		char* data, size_t size);

/**
 * schedi_job_epollsocket_read() - Consume buffered data from the socket.
 * @socket: The epoll socket to read from.
 * @data: Buffer to copy the data into.
 * @size: Size of @data.
 *
 * Copies as much buffered data as is available into @data from the socket's
 * read ring buffer. Nothing is received from the fd; the epoll system fills
 * the buffer via schedi_job_epollsocket_read_return().
 *
 * Must only be called from the thread that creates the epoll socket, i.e. the
 * job that owns it. Not thread-safe.
 *
 * Return: Number of bytes consumed, at most @size.
 */
int schedi_job_epollsocket_read(struct schedi_job_epollsocket* socket, 
		char* data, size_t size);

/**
 * schedi_job_epollsocket_write_return() - Flush the write buffer to the socket.
 * @socket: The epoll socket whose write buffer is drained.
 *
 * Sends as much buffered data as the socket accepts, advancing the write tail
 * by the amount sent.
 *
 * Return: Number of bytes sent on success. -1 on failure; the socket is
 * errored or shut down and can be closed and removed from the epoll list.
 * Note that -1 does not mean nothing was written: some bytes may already have
 * been flushed to the socket before the failure. The -1 return is not a
 * reliable indicator of the amount written, so treat any -1 as a dead socket.
 */
int schedi_job_epollsocket_write_return(struct schedi_job_epollsocket* socket);

/**
 * schedi_job_epollsocket_read_return() - Receive into the read buffer from the socket.
 * @socket: The epoll socket whose read buffer is filled.
 *
 * Receives as much data as the read buffer has space for, advancing the read
 * head by the amount received. Returns 0 if the buffer is already full.
 *
 * Return: Number of bytes received on success. -1 if the socket is errored or
 * shut down; the socket can be closed and removed from the epoll list.
 */
int schedi_job_epollsocket_read_return(struct schedi_job_epollsocket* socket);

/**
 * schedi_job_epollsocket_return() - Called by the main epoll loop after an
 * event.
 * @sock: The socket registration that triggered the event.
 * @events: The full epoll events bitmask reported for this fd (EPOLLIN,
 * EPOLLOUT, EPOLLERR, EPOLLHUP...).
 * 
 * EPOLLIN drains the socket into the read buffer via read_return(), EPOLLOUT
 * flushes the write buffer via write_return(). On EPOLLERR/EPOLLHUP the socket
 * is marked dead; buffered data is still drained first if EPOLLIN is set.
 * On death the socket is marked dead and -1 is returned; the caller epoll
 * maintainer then releases its reference via 
 * schedi_job_tool_epollsocket_done_() and removes the socket from the epoll
 * list.
 * 
 * Return: 0 on success and if socket is still alive. -1 if socket is dead. The
 * caller epoll maintainer will clean it up from the epoll list.
 */
int schedi_job_epollsocket_return(struct schedi_job_epollsocket* sock, int events);


/**
 * schedi_job_init() - Pre-initialise the array and cond_t.
 *
 * Called once at program startup, before any workers are spawned.
 * Initializes readyjob_cond, readyjob_mutex, sections, section_lock
 * respectively.
 *
 * Return: 0 on success, an error number on failure. 
 */
int schedi_job_init(void);

/**
 * schedi_job_deinit() - Destroy all array mutexes.
 *
 * Called once at program shutdown, after all workers have been joined.
 * Destroys readyjob_cond, readyjob_mutex, section_lock respectively.
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
 * here. With this, they are woken up and can shutdown now.
 * 
 * NOTE: When LOCKLESS_READYJOB is defined, workers never sleep on
 * readyjob_cond, making this function a no-op (returns 0 immediately).
 * 
 * Return: 0 on success, an error number on failure (always 0 with 
 * LOCKLESS_READYJOB).
 */
int schedi_wake_all_job_waiters();

/**
 * schedi_wake_all_job_waiters_lockless() - Wakes jobs without the
 * readyjob lock to fully gain control over lock.
 *
 * schedi_lock_readyjob_mutex() - Locks readyjob_mutex.
 *
 * schedi_unlock_readyjob_mutex() - Unlocks readyjob_mutex.
 *
 * There is a need of lock that should cover the worker shutdown indication. 
 * So to wake the workers and to indicate shutdown more safely, there needs a
 * lock above shutdown indication. This is the purpose for those functions.
 *
 * Return: 0 on success, non-zero on failure.
 */
int schedi_lock_readyjob_mutex();
int schedi_unlock_readyjob_mutex();
int schedi_wake_all_job_waiters_lockless();


/** 
 * About Marks
 * 
 * Unstable: indicates that it's drastically modified by something else. For
 * example a destroy.
 * 
 * alive: It is alive. A real job is inside this job and contains some values.
 * 
 * executing: A worker thread is currently executing this job.
 * 
 * wlldestroy: Destroy call has been called on this job. Everything else
 * should stay away from this job so when the destroy queue check called, 
 * it could be removed quickly. Or else, it will be pushed to the queue 
 * again and will wait.
 * 
 * destroynotent: Destroy couldn't be noted to the destroy queue. Queue could be
 * full. A memory leak could've happened here, and it is actually. With this
 * flag, it could be found if wanted. Destroy function also indicates this with 
 * return value but just in case, this flag could do its job someday.
 */


/**
 * schedi_job_mark_or() - Atomically or-set a flag if the meta_flag is
 * modifiable.
 * @job: The job's pointer.
 * @flag: The flag to set.
 * @banned_flags: Flags that must not be set for the operation to proceed.
 * @ban_access: If non-zero, also fail when the access count is non-zero.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_mark_or(struct schedi_job *job, unsigned int flag,
unsigned int banned_flags, bool ban_access);

/**
 * schedi_job_mark_or_readiness() - Marks readiness with setready trigger when
 * every readiness flag is newly set.
 *
 * Atomically flags the flag and re-evaluates the full readiness set — the
 * given flag plus the derived READYEPOLLTOOL (packed waiting_count == 0) and
 * READYEPOLLSOCK (packed available_sockets >= required_available_sockets)
 * bits — in the same meta_flag CAS. If in that atomic instruction every
 * required flag "became" flagged, schedi_job_setready() is triggered.
 * 
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_mark_or_readiness(struct schedi_job* job, 
		unsigned int flag, unsigned int banned_flags, bool ban_access);

/**
 * schedi_job_mark_or_default() - Marks the flag unless it is unstable.
 * @job: The job's pointer.
 * @flag: The flag to set.
 * 
 * Return: 0 on success, banned flags if any banned flags are encountered.
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
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_unmark_or(struct schedi_job *job, unsigned int flag,
                         unsigned int banned_flags, bool ban_access);

/**
 * schedi_job_mark_alive() - Mark the job as alive.
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_ALIVE. Fails if UNSTABLE is set.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_mark_alive(struct schedi_job *job);

/**
 * schedi_job_mark_executing() - Mark the job as executing.
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_EXECUTING. Fails if UNSTABLE or
 * EXECUTING is set.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
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
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_unmark_executing(struct schedi_job *job);

/**
 * schedi_job_mark_unstable() - Mark the job as unstable (being edited).
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_UNSTABLE. Fails if UNSTABLE, READY or EXECUTING
 * is already set, or if the access count is non-zero.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_mark_unstable(struct schedi_job *job);

/**
 * schedi_job_mark_wlldestroy() - Mark the job as "this will be destroyed 
 * soon".
 * @job: The job's pointer.
 *
 * Sets SCHEDI_JOB_METAFLAG_WLLDESTROY. Fails if UNSTABLE is already set. This
 * mark is an indication for system to stay away from this and leave this alone
 * so destroyer could do its job quickly when the time comes.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_mark_wlldestroy(struct schedi_job* job);

/**
 * schedi_job_unmark_wlldestroy() - Unmark the "will be destroyed" flag.
 * @job: The job's pointer.
 *
 * Clears SCHEDI_JOB_METAFLAG_WLLDESTROY. Fails if UNSTABLE is already set.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_unmark_wlldestroy(struct schedi_job* job);

/**
 * schedi_job_mark_access() - Increment the access reference count.
 * @job: The job's pointer.
 *
 * Fails if UNSTABLE or WLLDESTROY is set.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
 * Access flags if maximum access encountered.
 */
unsigned int schedi_job_mark_access(struct schedi_job *job);

/**
 * schedi_job_unmark_access() - Decrement the access reference count.
 * @job: The job's pointer.
 *
 * Fails if UNSTABLE is set.
 *
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_unmark_access(struct schedi_job *job);

/**
 * schedi_job_mark_ready() - Triggered by the stuff that job is waiting for.
 * @job: The job's pointer.
 * 
 * Fails if READY, UNSTABLE or WLLDESTROY is set.
 * 
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_mark_ready(struct schedi_job* job);

/** 
 * schedi_job_unmark_ready() - Triggered when job is started being executed.
 * @job: The job's pointer.
 * 
 * Fails if UNSTABLE or WLLDESTROY is set.
 * 
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_unmark_ready(struct schedi_job* job);

/** 
 * schedi_job_mark_readybasic() - Mark the job's basic readiness.
 * @job: The job's pointer.
 * 
 * Sets SCHEDI_JOB_METAFLAG_READYBASIC: the job is at rest and wants to
 * be run again. Set by the worker automatically after the job function
 * returns 1, or "outside of the system" by the job's owner after a return
 * 0 to resume the job under user control. For the job to become ready,
 * all the other readinesses should also be ready; it will not be flagged
 * READY until every one of them becomes ready. No further call is required.
 * 
 * Fails if UNSTABLE or WLLDESTROY is set.
 * 
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_mark_readybasic(struct schedi_job* job);

/** 
 * schedi_job_unmark_readybasic() - Clear the job's basic readiness.
 * @job: The job's pointer.
 * 
 * Clears SCHEDI_JOB_METAFLAG_READYBASIC. Triggered when the job is picked
 * up and starts being executed.
 * 
 * Fails if UNSTABLE or WLLDESTROY is set.
 * 
 * Return: 0 on success, banned flags if any banned flags are encountered.
 */
unsigned int schedi_job_unmark_readybasic(struct schedi_job* job);

/**
 * schedi_job_create() - Allocate and initialise a new job slot.
 * @context: Opaque pointer to job-local state (e.g. bump-allocator region).
 *           Passed back to @run on each invocation and to @dtor on teardown.
 * @run: Entry-point function. Called by a worker when the job is picked up.
 * 	Function should return 1 to imply working again and make READYBASIC
 * 	flagged. When everything else (epollsocket, epoll and associated
 * 	jobs) is ready, job will be marked ready and will be executed.
 * @dtor: Optional destructor for @context. Called by schedi_job_destroy or
 *        destroy_now regardless of whether the job completed or was
 *        torn down mid-flight. May be NULL.
 *
 * Claims a slot from the static array. section_claim() locks the
 * section_lock, finds a free slot, marks it UNSTABLE, and unlocks.
 * After all fields are initialised, SCHEDI_JOB_METAFLAG_ALIVE, READYEPOLLSOCK,
 * READYEPOLLTOOL, READYJOB are set altogether atomically.
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
 * Return: 0 on success, -1 if it couldn't be destroyed now but added to the 
 * queue, -2 if queue was full and couldn't be added also to the queue.
 */
int schedi_job_destroy(struct schedi_job *job);

/**
 * schedi_job_pickready() - Return a suspended job ready for execution.
 * 
 * If sleeping after finding no job is being executed, schedi_pickingreadyjob()
 * should be called before this function. If there's no job, then
 * schedi_waitforjob() can be called for sleeping. If there's a job returned,
 * schedi_pickedreadyjob() should be called to unlock the mutex.
 *
 * Pops a job from ready_job_cache and guarantees that job won't be UNSTABLE.
 * If a candidate is found, it is returned directly (the caller should call
 * schedi_job_mark_executing or similar before working on it). It also does
 * not unmark the job access if it found a successful job. After marking it
 * "executing", "accessing" should be removed (should be unmarked).
 *
 * Return: a pointer to a ready job, or NULL if none is ready.
 */
struct schedi_job *schedi_job_pickready(void);

/**
 * schedi_pickingreadyjob() - Locks the readyjob_mutex so the jobs that did get
 * checked won't be able to signal while rest of the jobs are being checked.
 */
void schedi_pickingreadyjob();

/**
 * schedi_waitforjob() - cond_wait with readyjob_cond.
 */
void schedi_waitforjob();

/**
 * schedi_pickedreadyjob() - A job is picked successfully. Unlocks the
 * readyjob_mutex.
 */
void schedi_pickedreadyjob();

/**
 * schedi_job_setready() - Sets job ready and signals readyjob_cond atomically.
 * 
 * Also caches the job to the array ready_jobs_cache. So there's no turning 
 * back from setting a job ready. Turning back is really inefficient to do 
 * again and again. Don't do that.
 * 
 * Return: 0 on success, -1 if couldn't be marked ready, -2 if caching failed (due
 * to cache array being full), and extra -10 gets added if mutex unlock failed 
 * if LOCKLESS_READYJOB built is not used.
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
 * Does not consume anything. If it's already indicated by a really old job, it will
 * be marked and exited immediately anyway.
 * 
 */
void schedi_job_completion_wait(struct schedi_job_completion_indicator* indicator);



/**
 * schedi_job_epoll_requests_list_destroy() - Free an epoll requests list.
 * @list: Pointer to a struct schedi_job_epoll_requests_list previously
 *        returned by schedi_job_epoll_requests_list_new().
 *
 * Frees @list. Outstanding schedi_job_epoll_request owners are not
 * invalidated here — stale requests are detected by generation mismatch
 * (req->job_gen vs. the slot's gen) in schedi_job_epoll_request_return.
 * The caller must not access @list after this call.
 */
void schedi_job_epoll_requests_list_destroy(struct schedi_job_epoll_requests_list *list);

/**
 * schedi_job_destroy_now() - Internal teardown.
 * @job: The job to destroy.
 *
 * Marks the slot UNSTABLE. On success increases gen, releases the slot back 
 * to the free pool, destroys the epoll list, calls the destructor, clears 
 * meta_flag, and returns 1. If mark_unstable fails (slot is busy) returns 0.
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
 * Tries this cycle 1 time for each element on the destroy queue on the time
 * that this gets called.
 */
void schedi_job_destroy_queue_check(void);

#endif /* SCHEDI_JOBS_H */
