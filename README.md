## schedi

schedi is an asynchronous multi-thread job scheduling and executing
system/library with integration to epoll.

Serves:
- job creation, 
- waiting a job to be completed without consuming any CPU,
- making a job wait a while until a required amount of data is received from a
socket, 
- timing out a socket if that socket is not becoming ready for a specific
amount of time,
- making a job wait a until the specified jobs become ready.

### Requirements

- ```pthread```
- ```linux epoll```: Integration with epoll mechanism requires linux epoll
but it is easily pullable out from the codebase.

### Compiling

To compile it as a static library inside the schedi folder

```
make lib
```

then a source named ```program.c```, if schedi is at folder ```schedi```,
with:

```bash
gcc -I schedi/include program.c schedi/libschedi.a -pthread
```



#### Compile Time Definitions

These definitions should be defined compile time when the library is
being built.

##### LOCKLESS_READYJOB

Define this on compilation to not put workers to sleep when they didn't 
found a job. Normally workers sleep until there is a job. This sleep 
approach depends heavily on mutexes. For constantly checking if theres 
a job or not and use lockless ready job caches etc, use this.

##### SCHEDI_EXT_TIMEOUT

Define this on compilation to enable the timeout extension for epollsocket
system. Details are at below.

### Usage

#### Initialize

Firstly, job and worker system should be initialized in order.


```C
if(schedi_job_init()) {
	return 1;
}

// initializing 16 worker thread.
if(schedi_worker_init(16)) {
	return 2;
}
```

In the end, they should be deinitialized. Worker should be deinitialized
first because a worker could be using a job while on deinitialization of 
jobs.

```C
if(schedi_worker_deinit()) {
	printf("Worker Deinit Failed\n");
	return 3;
}

if(schedi_job_deinit()) {
	printf("Job Deinit Failed\n");
	return 4;
}
```

#### Defining a Job

Define a job context struct. Could be in any name, with any variables.
When a job gets created, it will get the ```&the_context``` as parameter
and that will be contained in the job's self. Nothing touches to the 
context except "the job function" (```example_job_function``` in a bit
bottom).

```C
struct example_job_context {
	int information1;
	int phase;
} the_context;
```

Define the job function to be called. Could be in any name. But 
returning integer and containing only one parameter as a
```struct schedi_job*``` is required.

```C
int example_job_function (struct schedi_job* job)
{
	// this is the context. context can be played with.
	struct example_job_context* context = (struct example_job_context*)job->context; 

	switch(context->phase) {
	case 0:
		context->information1 = 2000;
		context->phase = 1;
		break;
	case 1:
		context->information1 *= 2;
		context->phase = 2;
		break;
	case 2:
		context->information1 -= 1980;
		context->phase = 3;
		break;
	case 3:
		context->information1 = 0;
		return -1; 	// -1 means that when worker is done with the job, it will
					// call the destroy function. But that function does not
					// guarantee destroying immediately. It will try, but
					// it could be scheduled in cases.
					//
					// Also it will trigger the indicator if its set when
					// -1 is returned, indicating the completion.

	}

	return 1; 	// returning 1 makes READYBASIC marked immediately after exiting
                // the function. If everything else is ready (such as requests
                // of the epoll tool), job will be ready to proceed further and 
                // will be called again on the closest time.

	return 0;	// returning 0 makes READYBASIC not marked. As marking
                // READYBASIC from outside of the system an approach to
                // consider, it's dangerous due to race of destroy.
}
```

#### Creating the Job and Running It

Create the job.

```C
struct schedi_job* job = schedi_job_create(
			(void*)&the_context, example_job_function, NULL);
```

Instead of NULL there, you can pass a function ```void dtor(void*context);``` 
that will be called on destruction. This function will get the context as parameter 
and could be used for destruction of context.

To make job start running, it should be setted ready. When a job is ready, a worker
that is empty and waiting or searching for work will get this job and execute it.

```C
schedi_job_setready(job);
```

If getting generation id of job is wanted, getting the generation id before
```schedi_job_setready()``` call is advised and otherwise job would start
being processed and job generation id could be inconsistent.


#### Initializing Indicator and Waiting for Indication

Indicator should be initialized and setted for the job before setting it
ready for guaranteed execution.

Initialize the indicator.

```C
struct schedi_job_completion_indicator indicator;
schedi_completion_indicator_init(&indicator);
```

Set the indicator to the job.

```C
job->completion_indicator = &indicator;
```

Then the job can be waited until its done. Wait does not consume any CPU.

```C
schedi_job_setready(job);
schedi_job_completion_wait(&indicator);
```

#### Tool:Epoll

On job ```job```, if a file descriptor ```fd``` for epoll event ```EPOLLIN``` wanted,
this call can be made inside of job function.

```C
schedi_job_tool_epoll(job, fd, EPOLLIN);
```

Note: file descriptor should be non-blocking.

Then when the epoll loop gets triggered with ```EPOLLIN``` for the file descriptor
```fd```, job will be setted ready.

#### Tool:Epoll Socket

On job ```job```, a file descriptor ```fd``` can be registered to the socket
system to make job ready if that file descriptor provided at least a number of
bytes or when it complies a minimum count of bytes requirement on writing.
Uses an inner buffer system.


In a job function, entry a socket as an epoll socket with:

```C
int minimum_read_buffer_count = 20;
int minimum_write_buffer_emptiness_count = 0;
struct schedi_job_epollsocket* socket = schedi_job_tool_epollsocket(
    job, 
    fd, 
    minimum_read_buffer_count,
    minimum_write_buffer_emptiness_count);
```

Here, ```socket``` should be saved so it will be used on read and write
operations. The example code above sets that socket will be ready when
20 bytes of data is ready to read from buffer and when at least 0 byte
of data can be written to the buffer (for write operations).

Then to set job's required available sockets (required ready socket count)
use:

```
schedi_job_required_available_socket(job, 1);
```

1 is the required available socket count. When at least 1 socket is ready,
socket readiness will be marked. And when everything else is ready too,
job will be ready to execute.

Then reading and writing is the same as the standard read() and write()
functions.

```
schedi_job_epollsocket_read(socket, data, 20);
schedi_job_epollsocket_write(socket, data, 20);
```

When the minimum counts are determined, those functions are guaranteed to be
used and successfully done as those counts when job became ready on those
filters. (such as if minimum_write_buffer_count = 20, 
schedi_job_epollsocket_write with 20 is guaranteed to work and output 20 if
this socket is waited to make job ready.) Also those functions are 
non-blocking. So when requested 20 byte is not ready, it will read less than 
20.

##### Destroy of Epoll System after use of Epoll Sockets

To mark a socket to be destroyed after use, call:

```
schedi_job_epollsocket_done(socket);
```

Then socket will be accepted as not ready until it is destroyed. To be sure
socket is destroyed before exiting a job, set ```required_available_socket```
with counting this socket too.

#### Tool:Jobs

Just like how Epoll Sockets or Epolls can be waited to make a job ready, a job
can also be waited. Assuming ```parent``` is a parent job that is waiting
```job1```, ```job2``` and ```job3```; by using this scheme on the parent's
job function:

```C
schedi_job_tool_job(parent, job1);
schedi_job_tool_job(parent, job2);
schedi_job_tool_job(parent, job3);
schedi_job_setready(job1);
schedi_job_setready(job2);
schedi_job_setready(job3);
schedi_job_required_available_jobs(parent, 3);
return 1;
```

the next execution of job function will be when at least 3 jobs completed among
the jobs specified with schedi_job_tool_job(). In this example, all of those
jobs needed to be completed to make this job function executed again.

#### TimeOut Extension

When library is compiled with ```-DSCHEDI_EXT_TIMEOUT```, epollsocket system
will gain some use and behaviour changes to use a timeout mechanism.

Entrying an epollsocket expands to

```
time_t timeout_time_in_seconds = 5;
schedi_job_tool_epollsocket(
    job,
    fd,
    minimum_read_buffer_count,
    minimum_write_buffer_emptiness_count,
    timeout_time_in_seconds);
```

In this example, 5 seconds of inactivity while socket is not ready will pop the 
socket out of epoll mechanism and will be counted as a ready socket.

To not use timeout in a socket with use of ```-DSCHEDI_EXT_TIMEOUT```, zero can
be putted to the timeout time to not arm the timer.

When a socket's time is out but socket was ready when the time out happened,
timer will be resetted and socket will continue existing on the epoll
mechanism.

To add a socket (that is popped out by the timeout) back to the epoll 
mechanism, use:

```
int schedi_job_epollsocket_epollagain(struct schedi_epollsocket *_socket);
```

Returns non-zero if epoll addition failed or socket was not containing epolldid
mark, indicating that epoll mechanism didn't popped it out.

#### Details on Destruction

Except context, everything belongs to the job is destructed after execution and the job
pointer should not be used unless it is guaranteed that its the same job as same pointers
could belong to other jobs. Guarantee can be made with ```job->gen```. This is a generation
id and increases when a destructive operation has been made to the job. So for every 
pointer, every gen represents a unique job.

### Tests and Benefits

schedi's intention is to get a beautiful asynchronous execution with different
jobs. But as a side effect, performance can also be gained by seperating a job
that has some independent parts from each other and could execute flawlessly as 
seperated jobs with executing them asynchronously.

#### 100 Million Double Vector Randomization and Dot Product

Problem is setting a vector on two seperate arrays and making a dot product
with them and writing to the first array's vector. Every array contains
storage for 100 million vector and this operation done with all the array.

The tested example code is ```examples/aparalleljob.c```. Code parses array
to parts and creates a job for each of them. Then marks them ready to make
them executed by schedi system.

The same problem is solved with single-thread approach on 
```examples/aparalleljob_mono.c``` also to compare them.

On tests, convention is [nT nJ] with T representing used worker threads that
gets a job and executes it and J representing jobs that the problem is divided
to.

##### AMD Ryzen 5 2600

- [1T 1J]: 0.34% average increase. (stdev=0.77)
- [2T 2J]: 44.29% average decrease. (stdev=0.64)
- [3T 3J]: 59.52% average decrease. (stdev=0.42)
- [4T 4J]: 67.43% average decrease. (stdev=0.43)
- [7T 7J]: 76.03% average decrease. (stdev=0.44)
- [10T 10J]: 80.14% average decrease. (stdev=0.26)
- [16T 16J]: 80.90% average decrease. (stdev=0.32)
- [16T 32J]: 80.90% average decrease. (stdev=0.29)
- [32T 32J]: 81.48% average decrease. (stdev=0.30)
- [64T 64J]: 81.73% average decrease. (stdev=0.27)
- [128T 128J]: 81.86% average decrease. (stdev=0.21)
- [1024T 1024J]: 38.92% average increase. (stdev=5.50)
- [16T 1024J]: 39.30% average increase. (stdev=5.84)

##### Intel Pentium N3710

- [1T 1J]: 9.6% average increase. (stdev=2.4)
- [2T 2J]: 44.6% average decrease. (stdev=0.2)
- [3T 3J]: 60.9% average decrease. (stdev=0.3)
- [4T 4J]: 69.5% average decrease. (stdev=0.3)
- [7T 7J]: 63.5% average decrease. (stdev=0.7)
- [10T 10J]: 66.2% average decrease. (stdev=0.9)
- [16T 16J]: 66.9% average decrease. (stdev=1.4)
- [32T 32J]: 66.0% average decrease. (stdev=1.5)
- [64T 64J]: 64.7% average decrease. (stdev=1.0)
- [128T 128J]: 64.7% average decrease. (stdev=1.0)

### Flaws

#### Job Ready Cache Being Full

There's an indication flag for that. And jobs dont get the "READY" flag unless
they are cached but there's no way to get the non-cached jobs and regularly
try them to being cached. I sensed that its meaningless as that thing
will be filled up too (unless it's a dynamic list!). Practically workers
should stop working for this to happen. And, do not do that. Why stopping
the workers? Workers should work. It's their job.

If theres a problem but couldnt being figured out, think about checking if
job cache is full or not. If it is, try triggering jobs to be cached by
touching them after a time.
