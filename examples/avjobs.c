/**
 * avjobs.c - Test job-to-job dependencies (schedi_job_tool_job).
 *
 * Main creates only the parent job. Parent phase 0 creates the leaves,
 * wires dependencies on itself, launches them, then suspends.
 * When both leaves complete, READYJOB transitions and parent resumes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <schedi/jobs.h>
#include <schedi/worker.h>

#define TESTING_SLEEP_AMOUNT 50000*8*4

#define LEAF_COUNT 2

struct schedi_job_completion_indicator parent_indicator;

struct leaf_ctx {
	int value;
};

struct parent_ctx {
	struct schedi_job *leaves[LEAF_COUNT];
	struct leaf_ctx leaf_ctxs[LEAF_COUNT];
	int sum;
	int seen;
};

int leaf_func(struct schedi_job *job)
{
	struct leaf_ctx *ctx = job->context;

	switch (job->phase) {
	case 0:
		usleep(TESTING_SLEEP_AMOUNT);
		ctx->value += 10;
		printf("leaf: phase 0, value=%d\n", ctx->value);
		job->phase = 1;
		return 1;
	case 1:
		usleep(TESTING_SLEEP_AMOUNT);
		ctx->value *= 2;
		printf("leaf: phase 1, value=%d\n", ctx->value);
		job->phase = 2;
		return 1;
	case 2:
		usleep(TESTING_SLEEP_AMOUNT);
		printf("leaf: done, final value=%d\n", ctx->value);
		return -1;
	}

	return -1;
}

int parent_func(struct schedi_job *job)
{
	struct parent_ctx *ctx = job->context;

	switch (job->phase) {
	case 0:
		for (int i = 0; i < LEAF_COUNT; i++) {
			ctx->leaf_ctxs[i].value = (i + 1) * 5;
			ctx->leaves[i] = schedi_job_create(
				&ctx->leaf_ctxs[i], leaf_func, NULL);
			schedi_job_tool_job(job, ctx->leaves[i]);
		}
		schedi_job_required_available_jobs(job, LEAF_COUNT);

		for (int i = 0; i < LEAF_COUNT; i++)
			schedi_job_setready(ctx->leaves[i]);

		printf("parent: launched %d leaves, waiting...\n", LEAF_COUNT);
		job->phase = 1;
		return 1;

	case 1:
		for (int i = 0; i < LEAF_COUNT; i++) {
			struct leaf_ctx *lc = ctx->leaves[i]->context;
			ctx->sum += lc->value;
			ctx->seen++;
		}
		printf("parent: gathered %d leaves, sum=%d\n", ctx->seen, ctx->sum);
		return -1;
	}

	return -1;
}

int main(void)
{
	if (schedi_job_init()) {
		printf("job init failed\n");
		return 1;
	}
	if (schedi_worker_init(4)) {
		printf("worker init failed\n");
		return 2;
	}

	struct parent_ctx pctx = { .sum = 0, .seen = 0 };
	struct schedi_job *parent = schedi_job_create(&pctx, parent_func, NULL);
	if (!parent) {
		printf("parent create failed\n");
		return 3;
	}
	schedi_completion_indicator_init(&parent_indicator);
	parent->completion_indicator = &parent_indicator;

	schedi_job_setready(parent);

	schedi_job_completion_wait(&parent_indicator);

	printf("main: parent completed, sum=%d\n", pctx.sum);

	if (schedi_worker_deinit()) {
		printf("worker deinit failed\n");
		return 4;
	}
	if (schedi_job_deinit()) {
		printf("job deinit failed\n");
		return 5;
	}

	printf("PASS\n");
	return 0;
}
