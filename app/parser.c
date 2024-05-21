#include "parser.h"

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
