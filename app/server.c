#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "table.h"
#include "parser.h"
#include "command.h"

#define BUFFER_SIZE 1024
#define DEFAULT_PORT 6379

// TODO: cleanup thread
Table db;

void *handle_client(void *fd);
void send_command(int client_fd, char *buf, int len) {
    send(client_fd, buf, len, 0);
}


void print_raw_bytes(const char *msg, const char *data) {
    printf("%s ", msg);

    for (int i = 0; i < strlen(data); i++) {
        if (data[i] == '\n') {
            printf("\\n");
        } else if (data[i] == '\r') {
            printf("\\r");
        } else {
            printf("%c", data[i]);
        }
    }

    printf("\n");
}

bool is_option(char *option, char *name, char *short_name) {
    if (option == NULL) {
        return false;
    }

    if (strcmp(option, name) == 0) {
        return true;
    }

    if (short_name != NULL && strcmp(option, short_name) == 0) {
        return true;
    }

    return false;
}

int main(int argc, char **argv) {
  int port = DEFAULT_PORT;
  char *program = argv[0];

  for (int i = 1; i < argc + 1; i += 2) {
      if (is_option(argv[i], "--port", "-p")) {
          port = atoi(argv[i + 1]);
      } else if (is_option(argv[i], "--help", "-h")) {
          printf("Usage: %s [--port=<port>|-h] [--help|-h]\n", program);
          return 0;
      }
  }

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
      .sin_port = htons(port),
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

  init_table(&db);

  printf("%s server running on :%d\n", program, port);

  while (1) {
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);

    if (client_fd == -1) {
      printf("Could not accept connection from client.\n");
      return 1;
    }
    printf("Client %d connected\n", client_fd);
    int *p_client_fd = malloc(sizeof(int));
    *p_client_fd = client_fd;

    pthread_t t_id;
    pthread_create(&t_id, NULL, handle_client, (void *)p_client_fd);
  }

  close(server_fd);

  return 0;
}

void *handle_client(void *fd) {
  int client_fd = *(int *)(fd);
  free(fd);
  // TODO: a buffer may not be enough to read the whole message
  char buffer[BUFFER_SIZE];

  ssize_t bytes;
  while ((bytes = recv(client_fd, buffer, BUFFER_SIZE, 0))) {
    // TODO: how do we know that we have a full message in bytes?
    char *cursor = buffer;
    print_raw_bytes("Receive raw bytes", buffer);

    Message *message = parse_message(&cursor);
    printf("Message type: %d\n", message->type);
    RedisCommand *command = create_redis_command(message);
    printf("Command type: %d\n", command->type);

    char resp_buf[1024];
    int resp_len;

    if (command->type == RedisCommandEcho) {
        execute_echo_command(command, resp_buf, &resp_len);
    } else if (command->type == RedisCommandPing) {
        execute_ping_command(command, resp_buf, &resp_len);
    } else if (command->type == RedisCommandSet) {
        execute_set_command(&db, command, resp_buf, &resp_len);
    } else if (command->type == RedisCommandGet) {
        execute_get_command(&db, command, resp_buf, &resp_len);
    } else {
        printf("Unknown redis command\n");
        // TODO: send error
    }

    print_raw_bytes("Resp: bytes", resp_buf);
    send_command(client_fd, resp_buf, resp_len);

    // TODO: comment this and check how memory is growing
    free_message(message);
    free_redis_command(command);
  }

  if (bytes == -1) {
    close(client_fd);
    fprintf(stderr, "Receiving failed: %s\n", strerror(errno));
  }

  return NULL;
}
