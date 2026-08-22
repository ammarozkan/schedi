#include <schedi/log.h>
#include <stdatomic.h>
#include <sys/syscall.h>
#include <unistd.h>


#ifdef SCHEDI_LOG_NAMED_THREADS

#define SCHEDI_MAX_UNIQUE_THREAD_COUNT 32

const char* thread_names[] = {
	"Thread Adam", 
	"Thread Jonathan", 
	"Thread Mark", 
	"Thread Anzelmus", 
	"Thread Beckfield",
	"Thread Gus",
	"Thread McField",
	"Thread Harding",
	"Thread Jobs",
	"Thread Porter",
	"Thread Price",
	"Thread Crane",
	"Thread Reed",
	"Thread Avery",
	"Thread Hale",
	"Thread Cross",
	"Thread Bishop",
	"Thread Grant",
	"Thread Lane",
	"Thread Ford",
};
unsigned int ntn = 0; // next target name. 

struct {pid_t pt; char* name;} ThreadNamings[SCHEDI_MAX_UNIQUE_THREAD_COUNT];

char* GetThreadName(pid_t pt)
{
	unsigned int i;
	for (i = 0 ; i < SCHEDI_MAX_UNIQUE_THREAD_COUNT ; i += 1) {
		if(ThreadNamings[i].pt == pt) return ThreadNamings[i].name;
		if(ThreadNamings[i].pt == 0) break;
	}
	ThreadNamings[i].pt = pt;
	ThreadNamings[i].name = thread_names[ntn++];
	return ThreadNamings[i].name;
}


#endif /* SCHEDI_LOG_NAMED_THREADS */



static _Atomic unsigned int flog_head;
static struct schedi_flog_event flog_events[SCHEDI_FLOG_SIZE];


static _Atomic unsigned int ferr_head;
static struct schedi_flog_event ferr_events[SCHEDI_FLOG_SIZE];

static void schedi_flog_explicit(const char* msg, int param, 
	_Atomic(unsigned int) *fh, struct schedi_flog_event *evnts)
{
	unsigned int idx = atomic_fetch_add_explicit(fh, 1, memory_order_acquire) 
		% SCHEDI_FLOG_SIZE;
	struct schedi_flog_event *ev = &evnts[idx];

	ev->tid = syscall(SYS_gettid);
	ev->msg = msg;
	ev->param = param;
#ifdef SCHEDI_LOG_NAMED_THREADS
	ev->name = GetThreadName(ev->tid);
#endif /* SCHEDI_LOG_NAMED_THREADS */
}

void schedi_flog(const char *msg, int param)
{
	schedi_flog_explicit(msg, param, &flog_head, flog_events);
}

void schedi_ferr(const char* msg, int param)
{
	schedi_flog(msg, param);
	schedi_flog_explicit(msg, param, &ferr_head, ferr_events);
}


void schedi_flog_get(struct schedi_flog_event* events)
{
	int i = 0;
	for(i = SCHEDI_FLOG_SIZE-1 ; i >= 0 ; i -= 1) {
		unsigned int j = (flog_head - (unsigned int)i)%SCHEDI_FLOG_SIZE;
		events[SCHEDI_FLOG_SIZE-1-i] = flog_events[j];
	}
}

void schedi_ferr_get(struct schedi_flog_event* events)
{
	int i = 0;
	for(i = SCHEDI_FLOG_SIZE-1 ; i >= 0 ; i -= 1) {
		unsigned int j = (ferr_head - (unsigned int)i)%SCHEDI_FLOG_SIZE;
		events[SCHEDI_FLOG_SIZE-1-i] = ferr_events[j];
	}
}
