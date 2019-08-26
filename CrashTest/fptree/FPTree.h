#ifndef STRING_TYPE
#ifndef FPTREE_H_
#define FPTREE_H_
namespace fptree {
#include <stdbool.h>

//#define NUM_LN_ENTRY   63
#define NUM_LN_ENTRY   58
#define NUM_IN_ENTRY   254
#define MIN_IN_ENTRIES (NUM_IN_ENTRY / 2)
#define MIN_LN_ENTRIES (NUM_LN_ENTRY / 2)
#define IS_FULL		((0x1UL << NUM_LN_ENTRY) - 1)
#define THIS_IN		1
#define THIS_LN		2

#define LOG_DATA_SIZE		48
#define LOG_AREA_SIZE		4294967296
#define LE_DATA			    0
#define LE_COMMIT		    1

#define BITS_PER_LONG	64

typedef struct entry entry;
typedef struct Internal_Node IN;
typedef struct Leaf_Node LN;
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

struct Internal_Node {
	unsigned char type;
	IN *parent;
	unsigned int nKeys;
	unsigned long keys[NUM_IN_ENTRY];
	void *leftmostPtr;
	void *ptr[NUM_IN_ENTRY];
};

struct Leaf_Node {
	unsigned char type;
	IN *parent;
	unsigned long bitmap;
	LN *pNext;
	entry entries[NUM_LN_ENTRY];
	unsigned char fingerprints[NUM_LN_ENTRY];
    unsigned char lock;
	//char dummy[8];
};

struct tree {
	void *root;
	log_area *start_log;
};

tree *initTree();
void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[]);
void *Lookup(tree *t, unsigned long key);
int Search(IN *curr, unsigned long key);
void *find_leaf_node(void *curr, unsigned long key);
void Insert(tree *t, unsigned long key, void *value);
void *Update(tree *t, unsigned long key, void *value);
int insert_in_leaf_noflush(LN *curr, unsigned long key, void *value);
void insert_in_leaf(LN *curr, unsigned long key, void *value);
void insert_in_inner(IN *curr, unsigned long key, void *value);
void insert_in_parent(tree *t, void *curr, unsigned long key, void *splitNode);
int Delete(tree *t, unsigned long key);

static inline unsigned long __ffs(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "rm" (word));
	return word;
}

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

}
#endif

#else
#ifndef FPTREE_H_
#define FPTREE_H_
namespace fptree {
#include <stdbool.h>
#define NUM_LN_ENTRY	58
#define NUM_IN_ENTRY	254
#define MIN_IN_ENTRIES (NUM_IN_ENTRY / 2)
#define MIN_LN_ENTRIES (NUM_LN_ENTRY / 2)
#define IS_FULL		((0x1UL << NUM_LN_ENTRY) - 1)
#define THIS_IN		1
#define THIS_LN		2

#define LOG_DATA_SIZE		48
#define LOG_AREA_SIZE		4294967296
#define LE_DATA			    0
#define LE_COMMIT		    1

#define BITS_PER_LONG	64

typedef struct entry entry;
typedef struct Internal_Node IN;
typedef struct Leaf_Node LN;
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

struct Internal_Node {
	unsigned char type;
	IN *parent;
	unsigned int nKeys;
	key_item *keys[NUM_IN_ENTRY];
	void *leftmostPtr;
	void *ptr[NUM_IN_ENTRY];
};

struct Leaf_Node {
	unsigned char type;
	IN *parent;
	unsigned long bitmap;
	LN *pNext;
	entry entries[NUM_LN_ENTRY];
	unsigned char fingerprints[NUM_LN_ENTRY];
    unsigned char lock;
};

struct tree {
	void *root;
	log_area *start_log;
};

tree *initTree();
void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[]);
void *Lookup(tree *t, unsigned char *key, int key_len);
int Search(IN *curr, unsigned char *key, int key_len);
void *find_leaf_node(void *curr, unsigned char *key, int key_len);
void Insert(tree *t, unsigned char *key, int key_len, void *value);
void *Update(tree *t, unsigned long key, void *value);
int insert_in_leaf_noflush(LN *curr, key_item *new_item, void *value);
void insert_in_leaf(LN *curr, key_item *new_item, void *value);
void insert_in_inner(IN *curr, key_item *inserted_item, void *value);
void insert_in_parent(tree *t, void *curr, key_item *inserted_item, void *splitNode);
int Delete(tree *t, unsigned long key);

static inline unsigned long __ffs(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "rm" (word));
	return word;
}

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

}
#endif
#endif
