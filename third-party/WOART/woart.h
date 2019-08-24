#ifndef STRING_TYPE
#ifndef WOART_H
#define WOART_H
#include <stdint.h>
#include <stdbool.h>
#include <byteswap.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_PREFIX_LEN		6

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

typedef int(*woart_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

/**
 * path compression
 * partial_len: Optimistic
 * partial: Pessimistic
 */
typedef struct {
	unsigned char depth;
	unsigned char partial_len;
	unsigned char partial[MAX_PREFIX_LEN];
} path_comp;

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
    uint8_t type;
	path_comp path;
} woart_node;

typedef struct {
	unsigned char key;
	char i_ptr;
} slot_array;

typedef struct {
	unsigned long k_bits : 16;
	unsigned long p_bits : 48;
} node48_bitmap;

/**
 * Small node with only 4 children, but
 * 8byte slot array field.
 */
typedef struct {
    woart_node n;
	slot_array slot[4];
    woart_node *children[4];
} woart_node4;

/**
 * Node with 16 keys and 16 children, and
 * a 8byte bitmap field
 */
typedef struct {
    woart_node n;
	unsigned long bitmap;
    unsigned char keys[16];
    woart_node *children[16];
} woart_node16;

/**
 * Node with 48 children and a full 256 byte field,
 */
typedef struct {
    woart_node n;
    unsigned char keys[256];
    woart_node *children[48];
} woart_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    woart_node n;
    woart_node *children[256];
} woart_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;	
	unsigned long key;
} woart_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    woart_node *root;
    uint64_t size;
} woart_tree;

/*
 * For range lookup in NODE16
 */
typedef struct {
	unsigned char key;
	woart_node *child;
} key_pos;

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int woart_tree_init(woart_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_woart_tree(...) woart_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int woart_tree_destroy(woart_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_woart_tree(...) woart_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.

#ifdef BROKEN_GCC_C99_INLINE
# define woart_size(t) ((t)->size)
#else
inline uint64_t woart_size(woart_tree *t) {
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
void* woart_insert(woart_tree *t, const unsigned long key, int key_len, void *value);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* woart_delete(woart_tree *t, const unsigned char *key, int key_len);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* woart_search(const woart_tree *t, const unsigned long key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
woart_leaf* woart_minimum(woart_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
woart_leaf* woart_maximum(woart_tree *t);

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
int woart_iter(woart_tree *t, woart_callback cb, void *data);

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
int woart_iter_prefix(woart_tree *t, const unsigned char *prefix, int prefix_len, woart_callback cb, void *data);

void* woart_scan(woart_tree *t, unsigned long min, int num, unsigned long buf[]);

#ifdef __cplusplus
}
#endif
#endif

#else
#ifndef WOART_H
#define WOART_H
#include <stdint.h>
#include <stdbool.h>
#include <byteswap.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_PREFIX_LEN		6

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

typedef int(*woart_callback)(void *data, const unsigned char *key, uint32_t key_len, void *value);

/**
 * path compression
 * partial_len: Optimistic
 * partial: Pessimistic
 */
typedef struct {
	unsigned char depth;
	unsigned char partial_len;
	unsigned char partial[MAX_PREFIX_LEN];
} path_comp;

/**
 * This struct is included as part
 * of all the various node sizes
 */
typedef struct {
    uint8_t type;
	path_comp path;
} woart_node;

typedef struct {
	unsigned char key;
	char i_ptr;
} slot_array;

typedef struct {
	unsigned long k_bits : 16;
	unsigned long p_bits : 48;
} node48_bitmap;

/**
 * Small node with only 4 children, but
 * 8byte slot array field.
 */
typedef struct {
    woart_node n;
	slot_array slot[4];
    woart_node *children[4];
} woart_node4;

/**
 * Node with 16 keys and 16 children, and
 * a 8byte bitmap field
 */
typedef struct {
    woart_node n;
	unsigned long bitmap;
    unsigned char keys[16];
    woart_node *children[16];
} woart_node16;

/**
 * Node with 48 children and a full 256 byte field,
 * but a 128 byte bitmap field 
 * (4 bitmap group of 16 keys, 48 children bitmap)
 */
typedef struct {
    woart_node n;
	node48_bitmap bits_arr[16];
    unsigned char keys[256];
    woart_node *children[48];
} woart_node48;

/**
 * Full node with 256 children
 */
typedef struct {
    woart_node n;
    woart_node *children[256];
} woart_node256;

/**
 * Represents a leaf. These are
 * of arbitrary size, as they include the key.
 */
typedef struct {
    void *value;
    uint32_t key_len;	
	unsigned char key[];
} woart_leaf;

/**
 * Main struct, points to root.
 */
typedef struct {
    woart_node *root;
    uint64_t size;
} woart_tree;

/*
 * For range lookup in NODE16
 */
typedef struct {
	unsigned char key;
	woart_node *child;
} key_pos;

/**
 * Initializes an ART tree
 * @return 0 on success.
 */
int woart_tree_init(woart_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define init_woart_tree(...) woart_tree_init(__VA_ARGS__)

/**
 * Destroys an ART tree
 * @return 0 on success.
 */
int woart_tree_destroy(woart_tree *t);

/**
 * DEPRECATED
 * Initializes an ART tree
 * @return 0 on success.
 */
#define destroy_woart_tree(...) woart_tree_destroy(__VA_ARGS__)

/**
 * Returns the size of the ART tree.

#ifdef BROKEN_GCC_C99_INLINE
# define woart_size(t) ((t)->size)
#else
inline uint64_t woart_size(woart_tree *t) {
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
void* woart_insert(woart_tree *t, const unsigned char *key, int key_len, void *value);

/**
 * Deletes a value from the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* woart_delete(woart_tree *t, const unsigned char *key, int key_len);

/**
 * Searches for a value in the ART tree
 * @arg t The tree
 * @arg key The key
 * @arg key_len The length of the key
 * @return NULL if the item was not found, otherwise
 * the value pointer is returned.
 */
void* woart_search(const woart_tree *t, const unsigned char *key, int key_len);

/**
 * Returns the minimum valued leaf
 * @return The minimum leaf or NULL
 */
woart_leaf* woart_minimum(woart_tree *t);

/**
 * Returns the maximum valued leaf
 * @return The maximum leaf or NULL
 */
woart_leaf* woart_maximum(woart_tree *t);

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
int woart_iter(woart_tree *t, woart_callback cb, void *data);

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
int woart_iter_prefix(woart_tree *t, const unsigned char *prefix, int prefix_len, woart_callback cb, void *data);

void* woart_scan(woart_tree *t, const unsigned char *min, int key_len, int num, unsigned long buf[]);

#ifdef __cplusplus
}
#endif

#endif
#endif
