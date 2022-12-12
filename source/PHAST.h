#pragma once
#include "util.h"

#define USE_PMDK
#ifdef USE_PMDK
#include <libpmemobj.h>
#define PMEM_PATH "/mnt/pmem/PHAST/mempool"
#define POOL_SIZE (10737418240ULL) // pool size : 10GB
typedef struct SLOT_HEAD_ARRAY SHA;
typedef struct LeafSkipGroup LSG;

POBJ_LAYOUT_BEGIN(PHAST);
POBJ_LAYOUT_ROOT(PHAST, SHA);
POBJ_LAYOUT_TOID(PHAST, LSG)
POBJ_LAYOUT_END(PHAST);
#endif

#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)

#define CACHE_LINE_SIZE 64
#define MAX_U64_KEY 0xffffffffffffffffULL // max key in uint64_t

#define HEAD_COUNT 128 // the number of partitions.
#define HASH_KEY (MAX_U64_KEY / HEAD_COUNT)

#define USE_AGG_KEYS // index cache design.
#ifdef USE_AGG_KEYS
#define AGG_UPDATE_LEVEL 1    // if the height of an InnerSkipNode > AGG_UPDATE_LEVEL, put this node into the index cache.
#define AGG_SLOT_INIT_NUM 8   // the initial number of slots in the index cache
#define AGG_REDUNDANT_SPACE 4 // redundant space in case overflow.
class AGGIndex;
#endif

#define SPAN_TH 1 // for deterministic design of inner node

#define MAX_ENTRY_NUM 56                        // 56*1 (fingerprints) + 8 (bitmap) = 64 (cache line size)
#define GROUP_BITMAP_FULL 0x00ffffffffffffffULL // MAX_ENTRY_NUM capacity. 2^56-1
#define MAX_L 32                                // max level of InnerSkipNode
#define MAX_LEAF_CAPACITY 128                   // the max size of InnerSkipNode
#define MIN_LEAF_CAPACITY (MAX_LEAF_CAPACITY / 2)

// for unsigned long long only.
#define firstzero(x) __builtin_ffsll((~(x)))
// for unsigned long long only.
#define popcount1(x) __builtin_popcountll(x)

typedef struct alignas(16) Entry
{
    uint64_t key = 0;
    uint64_t value = 0;
} Entry;

typedef struct LeafSkipGroup
{
    alignas(64) uint64_t commit_bitmap = 0; // control read access to LN.
    uint8_t fingerprints[MAX_ENTRY_NUM];
    uint64_t max_key = 0;
    LeafSkipGroup *next;
    bool is_head;
    uint8_t pad[47];
    alignas(16) Entry entries[MAX_ENTRY_NUM];
} LSG;

typedef struct InnerSkipNode
{
    uint64_t max_key;
    RWMutex *locker;
#ifdef USE_AGG_KEYS // only head has agg_index.
    AGGIndex *agg_index;
#endif
    uint16_t nKeys;
    bool is_head;
    bool is_split; // indicate LB/LN is split.
    uint8_t nLevel;
    uint8_t pad[3];
    struct InnerSkipNode *next[MAX_L];
    uint64_t keys[MAX_LEAF_CAPACITY];
    LSG *leaves[MAX_LEAF_CAPACITY];
    uint64_t mem_bitmap[MAX_LEAF_CAPACITY];
} ISN;

#define new_node(n) ((ISN *)malloc(sizeof(ISN)))

typedef struct InnerSkipList
{
    uint8_t level[HEAD_COUNT];
    ISN *head[HEAD_COUNT];
} ISL;

typedef struct PHAST
{
    ISL *inner_list;
    int size;
} PHAST;

typedef struct SLOT_HEAD_ARRAY
{
    LSG *slot_head_array[HEAD_COUNT];
} SHA;

ISN *create_inner_node(int level);

PHAST *init_list();

////////////////////////////////////
// main functions
////////////////////////////////////

// REQUIRES: key and value are not 0.
// RETURN: true if succeeded. otherwise false.
bool Insert(PHAST *list, uint64_t key, uint64_t value);

// RETURN: value if succeeded. otherwise 0.
uint64_t Search(PHAST *list, uint64_t key);

void dram_free(PHAST *list);

PHAST *recovery(int n_thread);

// RETURN the old value if exist.
uint64_t Update(PHAST *list, uint64_t key, uint64_t newValue);

// delete key from inode, does not remove it,
// just reset the value to indicate this entry has been deleted.
// use MAX uint64_t as the indicator.
// THUS: only use update function to do this instead of a new function.
// RETURN the old value if exist.
uint64_t Delete(PHAST *list, uint64_t key);

// lock-free version.
int Range_Search(PHAST *list, uint64_t start_key, int num, uint64_t *buf);

////////////////////////////////////

// BRIEF: search key in skiplist and return the target inner node with
//        pre_nodes which is previous to the returned node in the search
//        path for the new inserted nodes's pre nodes, similarly next_nodes.
// RETURN: header node if the inner_list is empty, otherwise the target node.
ISN *SearchList(ISL *inner_list, uint64_t key, ISN *pre_nodes[], ISN *next_nodes[]);

// BRIEF: same as previous one, but do not record the pre/next-nodes.
ISN *SearchList(ISL *inner_list, uint64_t key, uint64_t *target_maxkey, bool lock = false);

// BRIEF: the key is belong to a new inner node, this function is to find
//        the previous and next node according this key and the level.
void FindUpdateNodeForLevel(uint64_t key, int level,
                            ISN **pre_node, ISN **next_node);

// REQUIRES: hold inode's read lock that make sure no split in accessing.
// RETURN: 0 if succeeded. +1 if need get the target inode again. -1 if failed.
int InsertIntoINode(ISN *inode, uint64_t key, uint64_t value,
                    ISN *pre_nodes[], ISN *next_nodes[]);

// BRIEF: used to install a new inner node / leaf block.
void InstallNewInnerNode(ISN *old_in, ISN *new_in,
                         ISN *pre_nodes[], ISN *next_nodes[]);

// REQUIRES: hold inode's read lock that make sure no split in accessing.
// BRIEF: thread safe if hold read lock.
// RETURN: the target value. 0 if not found.
uint64_t SearchINode(ISN *inode, uint64_t key);

bool TryToGetWriteLock(ISN *inode, const bool is_split);

int randomLevel();

void free_inner_list(PHAST *list);

void nv_free(PHAST *list);

int find_zero_bit(uint64_t x, uint16_t size);

void update_agg_keys(ISN *head, const int head_idx);

ISN *find_in_agg_keys(ISN *head, const uint64_t key, uint64_t *target_maxkey);

void print_list_all(PHAST *list, uint64_t key);
void print_list_all(PHAST *list);
void print_list_all(ISN *header);
void print_inode_and_next(ISN *node);
void print_lnode_all(LSG *node);
void print_lnode_all(LSG *node, uint64_t maxkey, const uint64_t bitmap);
void print_lnode_and_next(LSG *node);
void print_list_skeleton(PHAST *list);
void print_list_skeleton(ISN *header);
void print_mem_nvm_comsumption(PHAST *list);

static void for_debug()
{
    sleep(1);
}

#ifdef USE_AGG_KEYS
// BRIEF: cannot be modified after construct.
class AGGIndex
{
public:
    // REQUIRES: num must be larger than the number of expected nodes in this head.
    AGGIndex(ISN *head, const size_t num)
        : agg_cap(num),
          agg_num(0)
    {
        assert(head->is_head);
        agg_keys.reserve(agg_cap);
        agg_nodes.reserve(agg_cap);
        ISN *cursor = head->next[AGG_UPDATE_LEVEL];

        while (cursor != NULL && !cursor->is_head)
        {
            agg_keys.push_back(cursor->max_key);
            agg_nodes.push_back(cursor);
            agg_num++;
            cursor = cursor->next[AGG_UPDATE_LEVEL];
        }
    }

    ~AGGIndex()
    {
        agg_keys.clear();
        agg_nodes.clear();
    }

    inline size_t NewSize() const
    {
        return (agg_num + AGG_REDUNDANT_SPACE > agg_cap) ? (agg_cap * 2) : (agg_cap);
    }

    inline size_t Cap() const { return agg_cap; }

    // BRIEF: thread safe.
    // RETURN: return a inner node according the gaven key.
    //         NULL if failed.
    ISN *Find(const uint64_t key, uint64_t *target_maxkey, bool debug_info = false) const
    {
        // binary search.
        int low = 0, high = agg_num, mid = 0;
        while (low < high)
        {
            if (key <= agg_keys[low])
            {
                break;
            }
            mid = (low + high) / 2;
            // if (debug_info) fprintf(stderr, "key: %lu; low: %d; mid: %d; hig: %d; %lu, %lu, %lu\n",
            //                         key, low, mid, high, agg_keys[low], agg_keys[mid], agg_keys[high]);
            if (agg_keys[mid] > key)
            {
                high = mid;
            }
            else if (agg_keys[mid] < key)
            {
                low = mid + 1;
            }
            else
            {
                break;
            }
        }
        if (low > mid)
        {
            mid = low;
        }
        assert(mid <= agg_num);
        --mid;
        if (mid < 0)
            return NULL;
        *target_maxkey = agg_keys[mid];
        // if (agg_nodes[mid] == NULL) {
        //     for_debug();
        //     if (debug_info) exit(1);
        //     fprintf(stderr, "%lu; %lu; %lu; %lu; %lu; %lu\n", key, agg_num, mid, agg_keys[mid-1], agg_keys[mid], agg_keys[mid+1]);
        //     fprintf(stderr, "%p; %p; %p\n", agg_nodes[mid-1], agg_nodes[mid], agg_nodes[mid+1]);
        //     for (size_t x = 0; x < agg_keys.size(); ++x) {
        //         fprintf(stderr, "%d: %lu\n", x, agg_keys[x]);
        //     }
        //     fprintf(stderr, "\n");
        //     this->Find(key, target_maxkey, true);
        //     exit(1);
        // }
        return agg_nodes[mid];
    }

    // seq search.
    ISN *SeqFind(const uint64_t key, uint64_t *target_maxkey, bool debug_info = false) const
    {
        const size_t max = agg_num;
        size_t i = 0;
        if (max == 0)
            return NULL;
        for (i = 0; i < max; ++i)
        {
            if (key <= agg_keys[i])
            {
                break;
            }
        }

        assert(i <= max);
        if (i == 0)
        {
            return NULL;
        }
        else
        {
            --i;
        }
        *target_maxkey = agg_keys[i];
        return agg_nodes[i];
    }

public:
    const size_t agg_cap;           // capacity of this array.
    size_t agg_num;                 // number of keys in the array.
    std::vector<uint64_t> agg_keys; // agg keys
    std::vector<ISN *> agg_nodes;   // corresponding inner nodes
};
#endif
