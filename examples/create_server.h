#include <errno.h>
// server creating function to use with examples.

int create_server_nonblock(char* ip, uint16_t port)
{
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
	if(fd < 0) return -1;

	int opt = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	struct sockaddr_in addr = { 
		.sin_family = AF_INET, 
		.sin_port = htons(port)
	};

	if(inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
		close(fd); return -2;
	}
	errno=0;
	if(bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
		printf("bind error with errno:%i\n",errno);
		close(fd); return -3;
	}
	if(listen(fd, SOMAXCONN) < 0) {
		close(fd); return -4;
	}
	return fd;
}

int create_server(char* ip, uint16_t port)
{
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if(fd < 0) return -1;

	int opt = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	struct sockaddr_in addr = { 
		.sin_family = AF_INET, 
		.sin_port = htons(port)
	};

	if(inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
		close(fd); return -2;
	}
	errno=0;
	if(bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
		printf("bind error with errno:%i\n",errno);
		close(fd); return -3;
	}
	if(listen(fd, SOMAXCONN) < 0) {
		close(fd); return -4;
	}
	return fd;
}
