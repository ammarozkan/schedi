## schedi

schedi is an asynchronous multi-thread job scheduling and executing
system/library.

Currently there's flaws but gonna talk about it and publish the current
code. I'll try to fix by time.


### Requirements

- ```pthread```
- ```linux epoll```: Code uses epolls. But epoll can also be extracted
as schedi does not depend on epolls. epoll is just a tool inside of
schedi.


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

	return 0;	// returning 0 makes READYBASIC not marked. Returning 0 can be
                // used to make job depend on something out of the system and
                // triggering from outside of schedi. To mark READYBASIC,
                // schedi_job_mark_readybasic() should be called. Job will be
                // ready to execute when the rest of the readinesses (such as 
                // epoll requests, and epoll sockets) are also marked.
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

#### Epoll Tool

On job ```job```, if a file descriptor ```fd``` for epoll event ```EPOLLIN``` wanted,
this call can be made inside of job function.

```C
schedi_job_tool_epoll(job, fd, EPOLLIN);
```

Note: file descriptor should be non-blocking.

Then when the epoll loop gets triggered with ```EPOLLIN``` for the file descriptor
```fd```, job will be setted ready.


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

Tests has ben made with ```Intel Pentium N3710```.

#### 100 Million Double Vector Randomization and Dot Product

Problem is setting a vector on two seperate arrays and making a dot product
with them and writing to the first array's vector. Every array contains
storage for 100 million vector and this operation done with all the array.

The tested example code is ```examples/aparalleljob.c```. Code parses array
to parts and creates a job for each of them. Then marks them ready to make
them executed by schedi system.

The same problem is solved with single-thread approach on 
```examples/aparalleljob_mono.c``` also to compare them.

#### 1 Threads 1 Jobs

Job seperated to 1 thread and 1 jobs. So job is not seperated. schedi is
just being used to call the function.

On 25 tests, 9.6% average increase on time with a standard deviation of 2.4.

#### 2 Threads 2 Jobs

Job seperated to 2 thread with 2 jobs. Job is seperated (as can be seen on
the code) by whole 2 memory parts to not spend time with distanced positions
on each thread.

On 25 tests, %44.6 average decrease on time with a standard deviation of 0.2.

#### 3 Threads 3 Jobs

Tested CPU contains 2 cores. 3 threads won't have a logical increase. But it
is important to see the schedi's impact on job scheduling calculation.

On 25 tests, %60.9 average decrease on time with a standard deviation of 0.3.

#### Rest of The Tests

- [4T 4J]: %69.5 average decrease, with stdev of 0.3
- [7T 7J]: %63.5 average decrease, with stdev of 0.7
- [10T 10J]: %66.2 average decrease, with stdev of 0.9
- [16T 16J]: %66.9 average decrease, with stdev 1.4
- [32T 32J]: %66.0 average decrease with stdev 1.5
- [64T 64J]: %64.7 average decrease with stdev 1.0
- [128T 128J]: %64.7 average decrease with stdev 1.0



### Flaws

#### epoll socket system is not tested yet

epoll socket system provides a persistent epoll registeration with specifying
required data amount for a socket to make a job ready is done but not tested
yet and may contain faulty parts.

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

#### shutdown race

Worker has an issue of racing with shutdown flag when sleeping is used 
for job waiting.
