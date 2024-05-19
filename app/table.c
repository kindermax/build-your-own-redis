#include <stdlib.h>
#include <string.h>

#include "table.h"

#define TABLE_MAX_LOAD 0.75
#define DEFAULT_CAPACITY 8

/* FNV-1a implementation of hash function */
static uint32_t hash_string(const char* key, int length) {
    uint32_t hash = 2166136261u;
    for (int i = 0; i < length; i++) {
        hash ^= (uint32_t)key[i];
        hash *= 16777619;
    }
    return hash;
}

void init_table(Table* table) {
    table->count = 0;
    table->capacity = 0;
    table->entries = NULL;
}

void free_table(Table* table) {
    for (int i = 0; i < table->capacity; i++) {
        free_key(table->entries[i].key);
    }
    free(table->entries);
    init_table(table);
}

static Entry* find_entry(Entry* entries, int capacity, Key *key) {
    uint32_t index = key->hash % capacity;
    Entry* tombstone = NULL;

    for (;;) {
        Entry* entry = &entries[index];
        if (entry->key == NULL) {
            if (entry->deleted) {
                // We found a tombstone. Will return it instead of next empty entry.
                if (tombstone == NULL) {
                    tombstone = entry;
                }
            } else {
                // Empty entry.
                if (tombstone) {
                    // We have a tombstone and can use it as a candidate entry
                    return tombstone;
                }
                return entry;
            }
        } else if (strcmp(entry->key->name, key->name) == 0) {
            // We found the key.
            return entry;
        }

        // linear probing/collision handling
        index = (index + 1) % capacity;
    }
}

static void adjust_capacity(Table* table, int capacity) {
    Entry* entries = malloc(sizeof(Entry) * capacity);
    // initialize new array
    for (int i = 0; i < capacity; i++) {
        entries[i].key = NULL;
        entries[i].value = NULL;
        entries[i].deleted = false;
    }

    table->count = 0;
    // reinsert old entries into new array
    for (int i = 0; i < table->capacity; i++) {
        Entry* entry = &table->entries[i];
        if (entry->key == NULL) continue;

        Entry* dest = find_entry(entries, capacity, entry->key);
        dest->key = entry->key;
        dest->value = entry->value;
        table->count++;
    }

    free(table->entries);

    table->entries = entries;
    table->capacity = capacity;
}

bool table_set(Table* table, Key* key, char* value) {
    if (table->count + 1 > table->capacity * TABLE_MAX_LOAD) {
        int capacity = table->capacity < DEFAULT_CAPACITY ? DEFAULT_CAPACITY : table->capacity * 2;
        adjust_capacity(table, capacity);
    }
    Entry* entry = find_entry(table->entries, table->capacity, key);
    bool isNewKey = entry->key == NULL;
    if (isNewKey && entry->value == NULL) table->count++;

    entry->key = key;
    entry->value = strdup(value);
    entry->deleted = false;
    return isNewKey;
}

bool table_get(Table* table, Key* key, char** value) {
    if (table->count == 0) return false;

    Entry* entry = find_entry(table->entries, table->capacity, key);
    if (entry->key == NULL) return false;

    long now = get_time_ms();
    if (entry->key->expire_at > 0 && entry->key->expire_at < now) {
        // making it a tombstone
        entry->deleted = true;
        return false;
    }

    *value = entry->value;
    return true;
}

bool table_delete(Table* table, Key* key) {
    if (table->count == 0) return false;

    Entry* entry = find_entry(table->entries, table->capacity, key);
    if (entry->key == NULL) return false;

    // Place a tombstone in the entry.
    entry->key = NULL;
    entry->value = NULL;
    entry->deleted = true;
    return true;
}

Key* new_key(const char* name) {
    int length = strlen(name);
    uint32_t hash = hash_string(name, length);
    Key *key = malloc(sizeof(Key));
    key->name = strdup(name);
    key->length = length;
    key->hash = hash;
    key->expire_at = 0;
    return key;
}

void free_key(Key *key) {
    free(key->name);
    free(key);
}
