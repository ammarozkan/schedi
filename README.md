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

then a source named ```program.c``` if schedi is at folder ```schedi``` with:

```bash
gcc -I schedi/include program.c schedi/libschedi.a -pthread
```



#### Compile Time Definitions

These definitions should be defined compile time when the library is
being built.

##### LOCKLESS_READYJOB

Define this on compilation to not put workers to sleep when they didnt 
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
first because a worker could be using a job while we deinitialize the jobs.

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
context except "the job function" (```example_job_function``` at a bit
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

	return 1; 	// returnin 1 makes job be ready for the next call immediately.

	return 0;	// returning 0 means "do not set it ready after executing."
				// this is used when job's readiness would be triggered by
				// something else, such as epoll tool.
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

For make job start running, it should be setted ready. When a job is ready, a worker
that is empty and waiting or searching for work will get this job and execute it.

```C
schedi_job_setready(job);
```


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

On job ```job```, if a file descriptor ```fd``` for epoll event ```EPOLLIN```,
this call can be made inside of job function.

```C
schedi_job_tool_epoll(job, fd, EPOLLIN);
```

Then when the epoll loop gets triggered with ```EPOLLIN``` for the file descriptor
```fd```, job will be setted ready. For this to work, job function should return 0
after epoll tool calls.


#### Details on Destruction

Except context, everything belongs to the job is destructed after execution and the job
pointer should not be used unless it is guaranteed that its the same job as same pointers
could belong to other jobs. Guarantee can be made with ```job->gen```. This is a generation
id and increases when a destructive operation has been made to the job. So for every 
pointer, every gen represents a unique job.



### Tests and Benefits

schedi's intention is to get a beautiful asynchronous execution with different
jobs. But as a side effect, performance can also be gained by seperating a job
that has some independent parts from each other and execute flawlessly to 
jobs and executing them asynchronously.

#### 100 Million Double Vector Randomization and Dot Product

Problem is setting a vector on two seperate arrays and making a dot product
with them and writing to the first array's vector. Every array contains
storage for 100 million vector and this operation done with all the array.

The tested example code is ```examples/aparalleljob.c```. Code parses array
to parts and creates a job for each of them. Then marks them ready to make
them executed by schedi system.

The same problem is solved with single-thread approach on 
```examples/aparalleljob_mono.c``` also to compare them.

##### 2 threaded schedi with whole job parsed into 2 jobs

I did 4 tests with

```
time ./bin/aparalleljob ; time ./bin/aparalleljob_mono
```

and on avarage of 45% with 0.9 standard deviation gain has been made
on time with 2 thread 2 jobs.

##### same with 3 thread, 3 job

Interestingly the same 4 tests resulted with 38.8% increase on
time with 14.2 standard deviation. I think there's a collision
of threads or something like that.

##### same with 4 thread, 4 job

55% increase on time with 15.7 standard deviation.

#### same with 6 thread, 6 job

32% increase on time with 9.9 standard deviation.

#### same with 12 thread, 12 job

15.5% increase on time with 4.3 standard deviation.

#### same with 24 thread, 24 job

46% increase on time with 14 standard deviation.

Also there was a case with 145% increase on time but only occured
once.



### Flaws

#### examples/aparalleljob

When dividing the job more than 2 jobs and initializing same amount of threads,
single-thread calculation is generally faster.

#### epoll tool race with returning 0 on job function

When epoll tool is called, epoll could be triggered in the way of returning 0
thus the try of making it ready to execute. This is faulty.

#### one time epoll

Currently epoll tool call only supports for one time check. Then the requested
file descriptor is removed from epoll list. I am considering, and partially designed
already, a continous socket system for that.

#### shutdown race

Worker has an issue of racing with shutdown flag when sleeping is used 
for job waiting.
