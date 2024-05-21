#ifndef redis_command_h
#define redis_command_h

#include "parser.h"
#include "table.h"

#define GEN_BULK_STRING(buf, msg, len) sprintf(buf, "$%lu\r\n%s\r\n", len, msg)

typedef enum {
    RedisCommandEcho,
    RedisCommandPing,
    RedisCommandSet,
    RedisCommandGet,
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
    int argc;
} RedisCommand;

RedisCommandType get_command_type(char *value);
RedisCommand *create_redis_command(Message *message);
void add_redis_arg(RedisCommand *command, RedisArg *arg);
RedisArg *create_redis_arg(const char *value);
void free_redis_args(RedisArg *args);
void free_redis_command(RedisCommand *command);

void execute_echo_command(RedisCommand *command, char *resp_buf, int *resp_len);
void execute_ping_command(RedisCommand *command, char *resp_buf, int *resp_len);
void execute_set_command(Table *db, RedisCommand *command, char *resp_buf, int *resp_len);
void execute_get_command(Table *db, RedisCommand *command, char *resp_buf, int *resp_len);

#endif //redis_command_h
