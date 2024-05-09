#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BUFFER_SIZE 1024

void *handle_client(void *fd);

int main() {
  // Disable output buffering
  setbuf(stdout, NULL);

  int server_fd, client_addr_len;
  struct sockaddr_in client_addr;

  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd == -1) {
    printf("Socket creation failed: %s...\n", strerror(errno));
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    printf("SO_REUSEADDR failed: %s \n", strerror(errno));
    return 1;
  }

  struct sockaddr_in serv_addr = {
      .sin_family = AF_INET,
      .sin_port = htons(6379),
      .sin_addr = {htonl(INADDR_ANY)},
  };

  if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0) {
    printf("Bind failed: %s \n", strerror(errno));
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    printf("Listen failed: %s \n", strerror(errno));
    return 1;
  }

  printf("Waiting for a client to connect...\n");
  client_addr_len = sizeof(client_addr);

  while (1) {
    // TODO: maybe accept should be in a loop?
    int client_fd =
        accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);

    if (client_fd == -1) {
      printf("Could not accept connection from client.\n");
      return 1;
    }
    printf("Client %d connected\n", client_fd);
    int *p_client_fd = malloc(sizeof(int));
    *p_client_fd = client_fd;

    pthread_t thr;
    if (pthread_create(&thr, NULL, handle_client, (void *)p_client_fd) != 0) {
      printf("Failed to handle client connection\n");
    };
    // TODO: join ?
  }

  close(server_fd);

  return 0;
}

void *handle_client(void *fd) {
  int client_fd = *(int *)(fd);
  free(fd);
  char buffer[BUFFER_SIZE];

  while (1) {
    const char *msg = "+PONG\r\n";
    if (recv(client_fd, buffer, BUFFER_SIZE, 0) == -1) {
      printf("Receive failed: %s\n", strerror(errno));
      break;
    };
    write(client_fd, msg, strlen(msg));
  }

  close(client_fd);
  return NULL;
}
