/**
 * accept_client function to use with examples
 */

#include <sys/socket.h>
#include <errno.h>


int accept_client_nonblock(int serverfd)
{
	errno = 0;
	int fd = accept4(serverfd, NULL, NULL, SOCK_NONBLOCK);
	printf("accept4 out %i with errno:%i\n",fd, errno);
	if (fd < 0) return -1;
	return fd;
}

int accept_client(int serverfd)
{
	errno = 0;
	int fd = accept(serverfd, NULL, NULL);
	printf("accept out %i with errno:%i\n",fd, errno);
	if (fd < 0) return -1;
	return fd;
}
