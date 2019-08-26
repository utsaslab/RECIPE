#ifndef STRING_TYPE
#ifndef WBTREE_H
#define WBTREE_H
namespace wbtree {
#include <stdbool.h>
#define NODE_SIZE 			63
#define SLOT_SIZE 			NODE_SIZE + 1
#define MIN_LIVE_ENTRIES 	NODE_SIZE / 2

#define LOG_DATA_SIZE		48
#define LOG_AREA_SIZE		4294967296
#define LE_DATA			0
#define LE_COMMIT		1

#define BITMAP_SIZE		NODE_SIZE + 1

typedef struct entry entry;
typedef struct node node;
typedef struct tree tree;

typedef struct {
	unsigned int size;
	unsigned char type;
	void *addr;
	char data[LOG_DATA_SIZE];
} log_entry;

typedef struct {
	log_entry *next_offset;
	char log_data[LOG_AREA_SIZE];
} log_area;

struct entry{
	unsigned long key;
	void *ptr;
};

struct node{
	char slot[SLOT_SIZE];
	unsigned long bitmap;
	struct entry entries[NODE_SIZE];
	struct node *leftmostPtr;
	struct node *parent;
	int isleaf;
//	char dummy[32];		//15
//	char dummy[16];		//31
	char dummy[48];		//63
};

struct tree{
	node *root;
	log_area *start_log;
};

tree *initTree();
void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[]);
void *Lookup(tree *t, unsigned long key);
void Insert(tree *t, unsigned long key, void *value);
void *Update(tree *t, unsigned long key, void *value);
int Delete(tree *t, unsigned long key);
}
#endif

#else

#ifndef WBTREE_H
#define WBTREE_H
namespace wbtree {
#include <stdbool.h>
#define NODE_SIZE 			63
#define SLOT_SIZE 			NODE_SIZE + 1
#define MIN_LIVE_ENTRIES 	NODE_SIZE / 2

#define LOG_DATA_SIZE		48
#define LOG_AREA_SIZE		4294967296
#define LE_DATA			0
#define LE_COMMIT		1

#define BITMAP_SIZE		NODE_SIZE + 1

typedef struct entry entry;
typedef struct node node;
typedef struct tree tree;

typedef struct {
	unsigned int size;
	unsigned char type;
	void *addr;
	char data[LOG_DATA_SIZE];
} log_entry;

typedef struct {
	log_entry *next_offset;
	char log_data[LOG_AREA_SIZE];
} log_area;

typedef struct {
	int key_len;
	unsigned char key[];
} key_item;

struct entry{
	key_item *key;
	void *ptr;
};

struct node{
	char slot[SLOT_SIZE];
	unsigned long bitmap;
	struct entry entries[NODE_SIZE];
	struct node *leftmostPtr;
	struct node *parent;
	int isleaf;
//	char dummy[32];		//15
//	char dummy[16];		//31
	char dummy[48];		//63
};

struct tree{
	node *root;
	log_area *start_log;
};

tree *initTree();
void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[]);
void *Lookup(tree *t, unsigned char *key, int key_len);
void Insert(tree *t, unsigned char *key, int key_len, void *value);
void *Update(tree *t, unsigned char *key, int key_len, void *value);
int Delete(tree *t, unsigned long key);
}
#endif
#endif
