/**
 * The same problem approached but with single-thread, without schedi for
 * comparison.
 */

#include <stdio.h>

#include "aparalleljob_parameters.h"


int main()
{

	for (unsigned int i = 0 ; i < VECTOR_COUNT ; i += 1) {
		vecs[i] = RANDOM_VECTOR;
		params[i] = RANDOM_VECTOR;
		vecs[i] = DotProduct(vecs[i], params[i]);
	}


	return 0;
}