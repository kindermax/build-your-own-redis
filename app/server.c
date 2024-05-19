#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BUFFER_SIZE 1024

// RESP protocol type
typedef enum {
  MessageTypeArray,
  MessageTypeBulk,
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

Message *parse_message(char **cursor);
Message *parse_array(char **cursor);
Message *parse_bulk(char **cursor);

Message *parse_message(char **cursor) {
  switch (*cursor[0]) {
  case '*': {
    return parse_array(cursor);
  }
  case '$': {
    return parse_bulk(cursor);
  }
  default:
    printf("Error symbol not allowed, exiting, %s\n", *cursor);
    return NULL;
  }
}

Message *parse_array(char **cursor) {
  char *input = *cursor;
  char *data = input + 1; // skip data type

  int len = 0;
  while (*data != '\r') {
    len = (len * 10) + (*data - '0');
    data++;
  }

  Message *message = malloc(sizeof(Message));
  message->type = MessageTypeArray;
  message->as.array.len = len;
  message->as.array.items = malloc(sizeof(Message *) * len);

  *cursor = data + 2; // skip data + \r\n

  for (int i = 0; i < len; i++) {
    message->as.array.items[i] = parse_message(cursor);
  }

  return message;
}

Message *parse_bulk(char **cursor) {
  char *input = *cursor;
  char *data = input + 1; // skip data type

  int len = 0;
  while (*data != '\r') {
    len = (len * 10) + (*data - '0');
    data++;
  }

  data += 2; // skip \r\n

  char *str = malloc(len + 1);
  memcpy(str, data, len);
  str[len] = '\0';

  Message *message = malloc(sizeof(Message));

  message->type = MessageTypeBulk;
  message->as.bulk.len = len;
  message->as.bulk.string = str;

  *cursor = data + len + 2; // skip data and \r\n

  return message;
}

void free_message(Message *message) {
  if (message->type == MessageTypeArray) {
    for (int i = 0; i < message->as.array.len; i++) {
      free_message(message->as.array.items[i]);
    }
    free(message->as.array.items);
    free(message);
  } else if (message->type == MessageTypeBulk) {
    free(message->as.bulk.string);
    free(message);
  }
}

typedef enum {
    RedisCommandEcho,
    RedisCommandPing,
    RedisCommandNone,
} RedisCommandType;

struct RedisArg {
    char *value;
    struct RedisArg *next;
};

typedef struct RedisArg RedisArg;

typedef struct {
    RedisCommandType type;
    RedisArg *args;
} RedisCommand;

RedisCommandType get_command_type(char *value);
void add_redis_arg(RedisCommand *command, RedisArg *arg);
RedisArg *create_redis_arg(const char *value);

RedisCommand *create_redis_command(Message *message) {
    if (message->type != MessageTypeArray || message->as.array.len < 1) {
        printf("Must be non-empty array");
        return NULL;
    }

    Message **elements = message->as.array.items;
    RedisCommandType command_type = get_command_type(elements[0]->as.bulk.string);

    RedisCommand *command = malloc(sizeof(RedisCommand));
    command->type = command_type;
    command->args = NULL;

    for (int i = 1; i < message->as.array.len; i++) {
        switch (elements[i]->type) {
            case MessageTypeBulk:
                add_redis_arg(command, create_redis_arg(elements[i]->as.bulk.string));
                break;
        }
        
    }

    return command;
}

RedisCommandType get_command_type(char *value) {
    if (strcmp(value, "ECHO") == 0) {
        return RedisCommandEcho;
    } else if (strcmp(value, "PING") == 0) {
        return RedisCommandPing;
    }

    return RedisCommandNone;
}

RedisArg *create_redis_arg(const char *value) {
    RedisArg *arg = malloc(sizeof(RedisArg));
    arg->value = strdup(value);
    arg->next = NULL;
    return arg;
}

void add_redis_arg(RedisCommand *command, RedisArg *arg) {
    if (command->args == NULL) {
        command->args = arg;
    } else {
        RedisArg *cur = command->args;
        while (cur->next != NULL) {
            cur = command->args->next;
        }
        cur->next = arg;
    }
}

void free_redis_args(RedisArg *args) {
    while (args != NULL) {
        RedisArg *next = args->next;
        free(args->value);
        free(args);
        args = next;
    }
}

void free_redis_command(RedisCommand *command) {
    if (command != NULL) {
        free_redis_args(command->args);
        free(command);
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
    Message *message = parse_message(&cursor);
    printf("Message type: %d\n", message->type);

    RedisCommand *command = create_redis_command(message);

    if (command->type == RedisCommandEcho) {
        size_t len = strlen(command->args->value);
        char msg[len + 1];
        sprintf(msg, "$%lu\r\n%s\r\n", len, command->args->value);
        send(client_fd, msg, strlen(msg), 0);
    } else if (command->type == RedisCommandPing) {
        send(client_fd, "+PONG\r\n", 7, 0);
    }

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
