#include <stdlib.h>

#include "command.h"
#include "table.h"

const char PING_MSG[7] = "+PONG\r\n";
const char OK_MSG[5] = "+OK\r\n";
const char NULL_BULK[5] = "$-1\r\n";

RedisCommand *create_redis_command(Message *message) {
    if (message->type != MessageTypeArray || message->as.array.len < 1) {
        printf("Must be non-empty array\n");
        return NULL;
    }

    Message **elements = message->as.array.items;
    RedisCommandType command_type = get_command_type(elements[0]->as.bulk.string);

    RedisCommand *command = malloc(sizeof(RedisCommand));
    command->type = command_type;
    command->args = NULL;
    command->argc = 0;

    printf("Args len %d\n", message->as.array.len);
    for (int i = 1; i < message->as.array.len; i++) {
        printf("Arg %d type %d, value %s\n", i, elements[i]->type, elements[i]->as.bulk.string);
        command->argc++;
        switch (elements[i]->type) {
            case MessageTypeBulk:
                add_redis_arg(command, create_redis_arg(elements[i]->as.bulk.string));
                break;
        }
    }

    RedisArg *arg = command->args;
    while (arg != NULL) {
        printf("New arg %s\n", arg->value);
        arg = arg->next;
    }
    return command;
}

RedisCommandType get_command_type(char *value) {
    if (strcmp(value, "ECHO") == 0) {
        return RedisCommandEcho;
    } else if (strcmp(value, "PING") == 0) {
        return RedisCommandPing;
    } else if (strcmp(value, "SET") == 0) {
        return RedisCommandSet;
    } else if (strcmp(value, "GET") == 0) {
        return RedisCommandGet;
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
            cur = cur->next;
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

// TODO: use table of function pointers
// TODO: maybe we do not need to pass client_fd here but to return ExecutionResult like msg and msg len to send
void execute_echo_command(RedisCommand *command, char *resp_buf, int *resp_len) {
    *resp_len = GEN_BULK_STRING(resp_buf, command->args->value, strlen(command->args->value));
}

void execute_ping_command(RedisCommand *command, char *resp_buf, int *resp_len) {
    *resp_len = sizeof(PING_MSG);
    memcpy(resp_buf, PING_MSG, *resp_len);
}

void execute_set_command(Table *db, RedisCommand *command, char *resp_buf, int *resp_len) {
    char *value = command->args->next->value;
    Key *key = new_key(command->args->value);
    if (command->argc == 4) {
        // PX
        char *opt = command->args->next->next->value;
        if (strcasecmp(opt, "px") == 0) {
            int expire_ms = atoi(command->args->next->next->next->value);
            key->expire_at = get_time_ms() + expire_ms;
        }
    }
    table_set(db, key, value);
    // TODO: insertion to db must be guarded by mutex
    *resp_len = sizeof(OK_MSG);
    memcpy(resp_buf, OK_MSG, *resp_len);
}

void execute_get_command(Table *db, RedisCommand *command, char *resp_buf, int *resp_len) {
    char *value;
    Key *key = new_key(command->args->value);
    // TODO: selection from db must be guarded by mutex ?
    bool found = table_get(db, key, &value);
    if (found) {
        *resp_len = GEN_BULK_STRING(resp_buf, value, strlen(value));
    } else {
        *resp_len = sizeof(NULL_BULK);
        memcpy(resp_buf, NULL_BULK, *resp_len);
    }
}
