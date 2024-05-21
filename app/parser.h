#ifndef redis_parser_h
#define redis_parser_h

#include <stdlib.h>
#include <memory.h>
#include <stdio.h>

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
void free_message(Message *message);

#endif //redis_parser_h
