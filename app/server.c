#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BUFFER_SIZE 1024

typedef enum {
  ARRAY,
  BULK,
} MessageType;

typedef struct Message Message;
typedef struct Array Array;

struct Array {
  int len;
  // array or pointers to message
  Message **items;
};

typedef struct {
  int len;
  char *string;
} Bulk;

struct Message {
  MessageType type;
  union {
    Array array;
    Bulk bulk;
  } as;
};

Message *parseMessage(char **cursor);
Message *parseArray(char **cursor);
Message *parseBulk(char **cursor);

Message *parseMessage(char **cursor) {
  switch (*cursor[0]) {
  case '*': {
    return parseArray(cursor);
  }
  case '$': {
    return parseBulk(cursor);
  }
  default:
    printf("Error symbol not allowed, exiting, %s\n", *cursor);
    return NULL;
  }
}

Message *parseArray(char **cursor) {
  char *input = *cursor;
  char *data = input + 1; // skip data type

  int len = 0;
  while (*data != '\r') {
    len = (len * 10) + (*data - '0');
    data++;
  }

  Message *message = malloc(sizeof(Message));
  message->type = ARRAY;
  message->as.array.len = len;
  message->as.array.items = malloc(sizeof(Message *) * len);

  *cursor = data + 2; // skip data + \r\n

  for (int i = 0; i < len; i++) {
    message->as.array.items[i] = parseMessage(cursor);
  }

  return message;
}

Message *parseBulk(char **cursor) {
  char *input = *cursor;
  char *data = input + 1; // skip data type

  int len = 0;
  while (*data != '\r') {
    len = (len * 10) + (*data - '0');
    data++;
  }

  data += 2; // skip \r\n

  // allocate memory for the string
  char *str = malloc(len + 1);
  // copy the string from the buffer into our new memory
  memcpy(str, data, len);
  str[len] = '\0';

  Message *message = malloc(sizeof(Message));

  message->type = BULK;
  message->as.bulk.len = len;
  message->as.bulk.string = str;

  *cursor = data + len + 2; // skip data and \r\n

  return message;
}

void freeMessage(Message *message) {
  if (message->type == ARRAY) {
    for (int i = 0; i < message->as.array.len; i++) {
      freeMessage(message->as.array.items[i]);
    }
    free(message->as.array.items);
    free(message);
  } else if (message->type == BULK) {
    free(message->as.bulk.string);
    free(message);
  }
}

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
    Message *message = parseMessage(&cursor);
    printf("Message type: %d\n", message->type);

    if (message->type == ARRAY) {
      if (message->as.array.len == 1) {
        // TODO: how to know that array contains BULK ? check type for each
        // element ?
        if (strcmp(message->as.array.items[0]->as.bulk.string, "PING") == 0) {
          send(client_fd, "+PONG\r\n", 7, 0);
        }
      } else if (message->as.array.len == 2) {
        if (strcmp(message->as.array.items[0]->as.bulk.string, "ECHO") == 0) {
          int len = message->as.array.items[1]->as.bulk.len;
          char *string = message->as.array.items[1]->as.bulk.string;
          char msg[len + 1];
          sprintf(msg, "$%d\r\n%s\r\n", len, string);
          send(client_fd, msg, strlen(msg), 0);
        }
      }
    }

    // TODO: comment this and check how memory is growing
    freeMessage(message);
  }

  if (bytes == -1) {
    close(client_fd);
    fprintf(stderr, "Receiving failed: %s\n", strerror(errno));
  }

  return NULL;
}
