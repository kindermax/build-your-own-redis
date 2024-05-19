#ifndef redis_table_h
#define redis_table_h

#include <stdint.h>
#include <stdbool.h>

#include "util.h"

typedef struct {
    char *name;
    int length;
    uint32_t hash;
    long expire_at;
} Key;

typedef struct {
    Key *key;
    char *value;
    bool deleted;
} Entry;

typedef struct {
    int count;
    int capacity;
    Entry* entries;
} Table;

void init_table(Table* table);
void free_table(Table* table);
bool table_get(Table* table, Key *key, char** value);
bool table_set(Table* table, Key *key, char* value);
bool table_delete(Table* table, Key *key);
void table_add_all(Table* from, Table* to);
Key* new_key(const char* name);
void free_key(Key *key);

#endif