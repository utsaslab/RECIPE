#ifndef STRING_TYPE

#ifndef WORT_H
#define WORT_H

#include <stdint.h>
#include <stdbool.h>
#include <byteswap.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_PREFIX_LEN      6

#if 0
#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif
#endif

typedef int(*wort_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
	unsigned char depth;
	unsigned char partial_len;
	unsigned char partial[MAX_PREFIX_LEN];
} wort_node;

/**
 * Full node with 16 children
 */
typedef struct {
    wort_node n;
	wort_node *children[16];
} wort_node16;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;
	unsigned long key;
} wort_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    wort_node *root;
    uint64_t size;
} wort_tree;

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int wort_tree_init(wort_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_wort_tree(...) wort_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int wort_tree_destroy(wort_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_wort_tree(...) wort_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.

#ifdef BROKEN_GCC_C99_INLINE
# define wort_size(t) ((t)->size)
#else
inline uint64_t wort_size(wort_tree *t) {
    return t->size;
}
#endif
*/

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* wort_insert(wort_tree *t, const unsigned long key, int key_len, void *value);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* wort_delete(wort_tree *t, const unsigned char *key, int key_len);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* wort_search(const wort_tree *t, const unsigned long key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
wort_leaf* wort_minimum(wort_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
wort_leaf* wort_maximum(wort_tree *t);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int wort_iter(wort_tree *t, wort_callback cb, void *data);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int wort_iter_prefix(wort_tree *t, const unsigned char *prefix, int prefix_len, wort_callback cb, void *data);

void Range_Lookup(wort_tree *t, unsigned long num, unsigned long buf[]);

#ifdef __cplusplus
}
#endif
#endif

#else

#ifndef WORT_H
#define WORT_H
#include <stdint.h>
#include <stdbool.h>
#include <byteswap.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_PREFIX_LEN      6

#if defined(__GNUC__) && !defined(__clang__)
# if __STDC_VERSION__ >= 199901L && 402 == (__GNUC__ * 100 + __GNUC_MINOR__)
/*
 * GCC 4.2.2's C99 inline keyword support is pretty broken; avoid. Introduced in
 * GCC 4.2.something, fixed in 4.3.0. So checking for specific major.minor of
 * 4.2 is fine.
 */
#  define BROKEN_GCC_C99_INLINE
# endif
#endif

typedef int(*wort_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
	unsigned char depth;
	unsigned char partial_len;
    unsigned char partial[MAX_PREFIX_LEN];
} wort_node;

/**
 * Full node with 16 children
 */
typedef struct {
    wort_node n;
	wort_node *children[16];
} wort_node16;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;	
	unsigned char key[];
} wort_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    wort_node *root;
    uint64_t size;
} wort_tree;

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int wort_tree_init(wort_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_wort_tree(...) wort_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int wort_tree_destroy(wort_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_wort_tree(...) wort_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.

#ifdef BROKEN_GCC_C99_INLINE
# define wort_size(t) ((t)->size)
#else
inline uint64_t wort_size(wort_tree *t) {
    return t->size;
}
#endif
*/

/**
 * Inserts a new value into the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @arg value Opaque value.
 * @return NULL if the item was newly inserted, otherwise
 * the old value pointer is returned.
 */
void* wort_insert(wort_tree *t, const unsigned char *key, int key_len, void *value);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* wort_delete(wort_tree *t, const unsigned char *key, int key_len);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* wort_search(const wort_tree *t, const unsigned char *key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
wort_leaf* wort_minimum(wort_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
wort_leaf* wort_maximum(wort_tree *t);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each. The call back gets a
 * key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int wort_iter(wort_tree *t, wort_callback cb, void *data);

/**
 * Iterates through the entries pairs in the map,
 * invoking a callback for each that matches a given prefix.
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * @arg t The tree to iterate over
 * @arg prefix The prefix of keys to read
 * @arg prefix_len The length of the prefix
 * @arg cb The callback function to invoke
 * @arg data Opaque handle passed to the callback
 * @return 0 on success, or the return of the callback.
 */
int wort_iter_prefix(wort_tree *t, const unsigned char *prefix, int prefix_len, wort_callback cb, void *data);

#ifdef __cplusplus
}
#endif
#endif

#endif
