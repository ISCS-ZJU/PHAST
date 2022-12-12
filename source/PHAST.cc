#include "PHAST.h"

#ifdef USE_PMDK
PMEMobjpool *pop; // global pmemobj pool
#endif

inline LSG *AllocNewLeafNode()
{
	TOID(LSG)
	leaf = TOID_NULL(LSG);
	POBJ_ZNEW(pop, &leaf, LSG);
	if (TOID_IS_NULL(leaf))
	{
		fprintf(stderr, "failed to create a LSG in nvmm.\n");
		exit(0);
	}

	return D_RW(leaf);
}

ISN *create_inner_node(int level)
{
	ISN *p = new_node(level);
	if (!p)
		return NULL;
	p->locker = new RWMutex;
	p->locker->Init();
	p->max_key = 0;
	p->is_head = false;
#ifdef USE_AGG_KEYS
	p->agg_index = NULL;
#endif
	p->is_split = false;
	p->nLevel = level;
	for (int i = 0; i <= level; i++)
	{
		p->next[i] = NULL;
	}
	p->nKeys = 0;
	for (int i = 0; i < MAX_LEAF_CAPACITY; ++i)
	{
		p->mem_bitmap[i] = 0;
	}
	return p;
}

static inline int file_exists(const char *filename)
{
	struct stat buffer;
	return stat(filename, &buffer);
}

ISL *create_inner_list()
{
	ISL *list = (ISL *)malloc(sizeof(ISL));
	if (list == NULL)
		return NULL;

	/* force-disable SDS feature during pool creation*/
	int sds_write_value = 0;
	pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

	// create pmemobj pool and create the root

	if (file_exists(PMEM_PATH) != 0)
	{
		printf("create new one.\n");
		if ((pop = pmemobj_create(PMEM_PATH, "PHAST", POOL_SIZE, 0666)) == NULL)
		{
			perror("failed to create pool.\n");
			return NULL;
		}
	}
	else
	{
		printf("open existing one.\n");
		if ((pop = pmemobj_open(PMEM_PATH, POBJ_LAYOUT_NAME(PHAST))) == NULL)
		{
			perror("failed to open pool.\n");
			return NULL;
		}
	}

	TOID(SHA)
	root = POBJ_ROOT(pop, SHA);
	assert(!TOID_IS_NULL(root));

	// init multiple header.
	ISN *head = NULL;
	for (int i = 0; i < HEAD_COUNT; i++)
	{
		head = create_inner_node(0);
		if (head == NULL)
		{
			fprintf(stderr, "Memory allocation failed for head!");
			free(list);
			return NULL;
		}
		head->is_head = true;
		list->head[i] = head;
		list->level[i] = 0;

		// create the first inner node for this head.
		ISN *node = create_inner_node(0);
		assert(node);
		head->next[0] = node;

		// set the max key as the upper bound of this head.
		if (UNLIKELY(i == HEAD_COUNT - 1))
		{
			node->max_key = MAX_U64_KEY;
		}
		else
		{
			node->max_key = (i + 1) * HASH_KEY;
		}

		// create the first leaf node for this inner node.
		LSG *slot = AllocNewLeafNode();
		slot->is_head = true; // is the first slot in this inner node.
		slot->max_key = node->max_key;
		node->nKeys = 1;
		node->keys[0] = node->max_key;
		node->leaves[0] = slot;

		// link the root and the first leaf node.
		D_RW(root)->slot_head_array[i] = slot;

		// L1: head[2]    ->    head[3] -> ... -> head[X]    ->    NULL
		// L0: head[2] -> IN -> head[3] -> ... -> head[X] -> IN -> NULL
		// PM: leafnode[2] -> leafnode[3] -> leafnode[x] -> NULL
		if (i > 0)
		{
			for (int j = 1; j < MAX_L; j++)
			{
				list->head[i - 1]->next[j] = list->head[i];
			}
			list->head[i - 1]->next[0]->next[0] = head;
			list->head[i - 1]->next[0]->leaves[0]->next = slot;
		}
		else if (i == HEAD_COUNT - 1)
		{
			for (int j = 1; j < MAX_L; j++)
			{
				list->head[i]->next[j] = NULL;
			}
			list->head[i]->next[0]->next[0] = NULL;
			list->head[i]->next[0]->leaves[0]->next = NULL;
		}

#ifdef USE_AGG_KEYS
		head->agg_index = new AGGIndex(head, AGG_SLOT_INIT_NUM);
#endif
	}
	// the last head's max key is +INF;

	return list;
}

PHAST *init_list()
{
	PHAST *list = (PHAST *)malloc(sizeof(PHAST));
	if (list == NULL)
		return NULL;
	list->size = 0;
	list->inner_list = create_inner_list();
	if (list->inner_list == NULL)
		return NULL;
	srand(time(0));

	return list;
}

// RETURN: [0, size) if succeeded, size if failed.
inline int find_zero_bit(uint64_t x, uint16_t size)
{
	int ret = firstzero(x);
	if (ret == 0)
		return size;
	return (ret - 1);
}

uint64_t sl_hash(uint64_t key)
{
	key = (~key) + (key << 21);
	key = key ^ (key >> 24);
	key = (key + (key << 3)) + (key << 8);
	key = key ^ (key >> 14);
	key = (key + (key << 2)) + (key << 4);
	key = key ^ (key >> 28);
	key = key + (key << 31);
	return key;
}

static inline uint8_t hashcode1B(uint64_t x)
{
	x ^= x >> 32;
	x ^= x >> 16;
	x ^= x >> 8;
	return (uint8_t)(x & 0x0ffULL);
}

static inline uint8_t hashcode_v1(uint64_t x)
{
	return (uint8_t)(x & 0x0ffULL);
}

uint8_t f_hash(uint64_t key)
{
	uint8_t hash_key = sl_hash(key) % 256;
	return hash_key;
}

void insertion_sort_entry(Entry *base, int num)
{
	int i, j;
	Entry temp;

	for (i = 1; i < num; i++)
	{
		for (j = i; j > 0; j--)
		{
			if (base[j - 1].key > base[j].key)
			{
				temp.key = base[j - 1].key;
				temp.value = base[j - 1].value;
				base[j - 1].key = base[j].key;
				base[j - 1].value = base[j].value;
				base[j].key = temp.key;
				base[j].value = temp.value;
			}
			else
				break;
		}
	}
}

// select [s, e] includes start and end elements.
void quick_select(Entry *entries, int k, int s, int e)
{
	if (s >= e)
		return;
	int i = s, j = e;
	Entry tmp = entries[s];
	while (i != j)
	{
		while (i < j && entries[j].key >= tmp.key)
			j--;
		if (i < j)
		{
			entries[i].key = entries[j].key;
			entries[i].value = entries[j].value;
		}
		while (i < j && entries[i].key <= tmp.key)
			i++;
		if (i < j)
		{
			entries[j].key = entries[i].key;
			entries[j].value = entries[i].value;
		}
	}
	entries[i] = tmp;
	if (i == k - 1)
		return;
	else if (i > k - 1)
		quick_select(entries, k, s, i - 1);
	else
		quick_select(entries, k, i + 1, e);
}

void quick_select_index(const Entry *entries, int *index, int k, int s, int e)
{
	if (s >= e)
		return;
	int i = s, j = e;
	int tmp = index[s];
	while (i != j)
	{
		while (i < j && entries[index[j]].key >= entries[tmp].key)
			j--;
		if (i < j)
		{
			index[i] = index[j];
		}
		while (i < j && entries[index[i]].key <= entries[tmp].key)
			i++;
		if (i < j)
		{
			index[j] = index[i];
		}
	}
	index[i] = tmp;
	if (i == k - 1)
		return;
	else if (i > k - 1)
		quick_select_index(entries, index, k, s, i - 1);
	else
		quick_select_index(entries, index, k, i + 1, e);
}

static inline int binary_search(ISN *node, uint64_t key)
{
	int low = 0, mid = 0;
	int high = node->nKeys;
	if (high == 0)
		return 0;
	while (low < high)
	{
		if (key <= node->keys[low])
			return low;
		mid = (low + high) / 2;
		if (node->keys[mid] > key)
		{
			high = mid;
		}
		else if (node->keys[mid] < key)
		{
			low = mid + 1;
		}
		else
		{
			break;
		}
	}
	if (low > mid)
		mid = low;
	return mid;
}

static inline int seq_search(ISN *node, uint64_t key)
{
	int pos = 0, high = node->nKeys;
	for (pos = 0; pos < high; ++pos)
	{
		if (key <= node->keys[pos])
		{
			return pos;
		}
	}
	return pos - 1;
}

bool TryToGetWriteLock(ISN *inode, const bool is_split)
{
	if (__sync_bool_compare_and_swap(&(inode->is_split),
									 false, true))
	{
		inode->locker->ReadUnlock();
		inode->locker->WriteLock();
		return true;
	}
	inode->locker->ReadUnlock();
	return false;
}

ISN *SearchList(ISL *inner_list, uint64_t key,
				ISN *pre_nodes[], ISN *next_nodes[])
{

	// pre->max_key < key <= next->max_key if it is not head.
	int head_idx = key / HASH_KEY;
	ISN *pre = inner_list->head[head_idx], *next = NULL, *target = NULL;
	assert(pre != NULL);
	if (head_idx < HEAD_COUNT - 1)
	{
		next = inner_list->head[head_idx + 1];
	}

	int height = pre->nLevel;
	assert(height >= 0 && height < MAX_L);
	uint64_t pre_maxkey, next_maxkey;
	ISN *starter = NULL;
	ISN *starter_next = NULL;
	int span = 0;

	for (int i = 0; i < MAX_L + 1; ++i)
	{
		pre_nodes[i] = pre;	  // this is the current head.
		next_nodes[i] = next; // this is the next head.
	}

	// search from top to bottom level.
	for (int level = height; level >= 0; --level)
	{
		span = 0;
		starter = pre;
		next = pre->next[level];
		pre_maxkey = pre->max_key;
		next_maxkey = (next && !next->is_head) ? next->max_key : 0;
		// next_maxkey == 0 means we reached the next head or the tail.

	level_retry:
		while (next_maxkey && next_maxkey < key)
		{
// pre->max_key < next->max_key < key <= ...
// move to the next node in the same level.
#if 1
			span++;
			if (span > SPAN_TH && level == next->nLevel)
			{
				// do something,now we have the read lock of next_node;
				if (level < MAX_L - 1)
				{ // fail a: level >=MAX_L-1 ,pass
					if (__sync_bool_compare_and_swap(&next->nLevel, level, level + 1))
					{ // fail b:other thread increase the level,pass
						// update the the link;
						starter_next = starter->next[level + 1];
						next->next[level + 1] = starter_next;
						bool success_flag = false;
						while (starter_next == NULL || starter_next->is_head || starter_next->max_key > next_maxkey)
						{ // fail c:there is a node growth between starter and next,pass
							if (__sync_bool_compare_and_swap(&starter->next[level + 1], starter_next, next))
							{
								// update the head's level;if fail,other increase the head's level,pass
								if (level + 1 > height && level + 1 < MAX_L)
									__sync_bool_compare_and_swap(&inner_list->head[head_idx]->nLevel, height, level + 1);
								success_flag = true;
								break; // success,deterministic design finished!;
							}
							starter_next = starter->next[level + 1];
							next->next[level + 1] = starter_next;
						}
						if (success_flag == false)
							__sync_bool_compare_and_swap(&next->nLevel, level + 1, level); // reset the level;
#ifdef USE_AGG_KEYS
						if (next->nLevel == AGG_UPDATE_LEVEL)
						{
							update_agg_keys(inner_list->head[head_idx], head_idx);
						}
#endif
					}
				}
				span = 0;
				starter = next;
			}
#endif
			pre = next;
			next = pre->next[level];
			pre_maxkey = pre->max_key;
			next_maxkey = (next && !next->is_head) ? next->max_key : 0;
		}

		// pre->max_key < key <= next->max_key.
		pre_nodes[level] = pre;
		next_nodes[level] = next;
	}

	if (next_maxkey == 0)
	{
		// means next == NULL or next is head which indicate pre's max_key is
		// changed due to split and the new node has not installed to the list
		// yet which causes pre's max_key != head's upper bound.
		// this issue will be processed in check again if statement.
		target = pre;
		pre_nodes[0] = pre;
		next_nodes[0] = next;
	}
	else
	{
		// reset pre_nodes and next_nodes in level 0.
		// target = pre_nodes[0] and target'max_key >= key.
		assert(next != NULL && !next->is_head);
		target = next;
		pre_nodes[0] = next;
		next_nodes[0] = next->next[0];
	}

	// obtain the read lock of the target node.
	target->locker->ReadLock();

	// check again in case target was splitting before get the read lock.
	if (next_maxkey != target->max_key || target->max_key < key)
	{
		while (target->max_key < key)
		{
			ISN *tmp = target;
			target = target->next[0];

			assert(target && !target->is_head);

			target->locker->ReadLock();
			tmp->locker->ReadUnlock();
		}
		// got the right bottom level node.
		assert(target != NULL && !target->is_head);
		pre_nodes[0] = target;
		next_nodes[0] = target->next[0];
	}

	return target;
}

ISN *SearchList(ISL *inner_list, uint64_t key, uint64_t *target_maxkey, bool lock)
{

	// pre->max_key < key <= next->max_key if it is not head.
	int head_idx = key / HASH_KEY;
	ISN *pre = inner_list->head[head_idx], *next = NULL, *target = NULL;
	assert(pre != NULL);

#ifdef USE_AGG_KEYS
	uint64_t next_maxkey;
	int height = pre->nLevel;
	target = find_in_agg_keys(pre, key, target_maxkey);
	if (target)
	{
		pre = target;
		height = AGG_UPDATE_LEVEL;
	}

	// search from agg level to bottom level.
	for (int level = height; level >= 0; --level)
	{
		next = pre->next[level];
		next_maxkey = (next && !next->is_head) ? next->max_key : 0;
		// next_maxkey == 0 means we reached the next head or the tail.

		while (next_maxkey && next_maxkey < key)
		{
			// pre->max_key < next->max_key < key <= ...
			// move to the next node in the same level.
			pre = next;
			next = pre->next[level];
			next_maxkey = (next && !next->is_head) ? next->max_key : 0;
		}
		// pre->max_key < key < next->max_key.
	}

	if (next_maxkey == 0)
	{
		// means next == NULL or next is head which indicate pre's max_key is
		// changed due to split and the new node has not installed to the list
		// yet which causes pre's max_key != head's upper bound.
		// this issue will be processed in check again if statement.
		target = pre;
	}
	else
	{
		// reset pre_nodes and next_nodes in level 0.
		// target = pre_nodes[0] and target'max_key >= key.
		assert(next != NULL && !next->is_head);
		target = next;
	}
	*target_maxkey = target->max_key;
#else
	uint64_t next_maxkey;
	int height = pre->nLevel;
	assert(height >= 0 && height < MAX_L);

	// search from top to bottom level.
	for (int level = height; level >= 0; --level)
	{
		next = pre->next[level];
		next_maxkey = (next && !next->is_head) ? next->max_key : 0;
		// next_maxkey == 0 means we reached the next head or the tail.

		while (next_maxkey && next_maxkey < key)
		{
			// pre->max_key < next->max_key < key <= ...
			// move to the next node in the same level.
			pre = next;
			next = pre->next[level];
			next_maxkey = (next && !next->is_head) ? next->max_key : 0;
		}
		// pre->max_key < key < next->max_key.
	}

	if (next_maxkey == 0)
	{
		// means next == NULL or next is head which indicate pre's max_key is
		// changed due to split and the new node has not installed to the list
		// yet which causes pre's max_key != head's upper bound.
		// this issue will be processed in check again if statement.
		target = pre;
	}
	else
	{
		// reset pre_nodes and next_nodes in level 0.
		// target = pre_nodes[0] and target'max_key >= key.
		assert(next != NULL && !next->is_head);
		target = next;
	}
	*target_maxkey = target->max_key;
#endif

	// obtain the read lock of the target node.
	if (lock)
	{
		target->locker->ReadLock();
	}

	// check again in case target was splitting before get the read lock.
	if (target->max_key < key)
	{
		while (target->max_key < key)
		{
			ISN *tmp = target;
			target = target->next[0];
			*target_maxkey = target->max_key;

			assert(target && !target->is_head);

			if (lock)
			{
				target->locker->ReadLock();
				tmp->locker->ReadUnlock();
			}
		}
		// got the right bottom level node.
		assert(target != NULL && !target->is_head);
	}

	return target;
}

int InsertIntoINode(ISN *inode, uint64_t key, uint64_t value,
					ISN *pre_nodes[], ISN *next_nodes[])
{
	// we have got the read lock which means thread safe to access inode's meta.
	inode->locker->AssertReadHeld();

	// search the target leaf node.
	int loc = binary_search(inode, key);
	LSG *lfnode = inode->leaves[loc];

	uint64_t wbitmap;
	// probe the empty slot of working bitmap.
	while ((wbitmap =
				__atomic_load_n(&(inode->mem_bitmap[loc]), __ATOMIC_CONSUME)) < GROUP_BITMAP_FULL)
	{
		assert(wbitmap < GROUP_BITMAP_FULL);

		// get empty working bitmap slot.

		int slot = find_zero_bit(wbitmap, MAX_ENTRY_NUM);

		uint64_t new_wbitmap = wbitmap | (1ULL << slot);

		// check whether this slot has been assigned to other thread.
		if (__sync_bool_compare_and_swap(&(inode->mem_bitmap[loc]),
										 wbitmap, new_wbitmap))
		{
			// this slot has been assigned to this thread.
			// install KV to this slot.
			uint8_t fp = f_hash(key);
			lfnode->entries[slot].key = key;
			lfnode->entries[slot].value = value;
			lfnode->fingerprints[slot] = fp;

			// flush the KVpairs.
			pmemobj_persist(pop, &lfnode->entries[slot], sizeof(Entry));

			uint64_t cbitmap = __atomic_load_n(&(lfnode->commit_bitmap),
											   __ATOMIC_CONSUME);

			while (true)
			{
				// install commit bitmap.
				assert(cbitmap < GROUP_BITMAP_FULL);

				uint64_t ret_cbitmap = 0;
				uint64_t new_cbitmap = cbitmap | (1ULL << slot);
				if ((ret_cbitmap = __sync_val_compare_and_swap(&(lfnode->commit_bitmap),
															   cbitmap, new_cbitmap)) != cbitmap)
				{
					// install failed: a. cbitmap's other slot has been changed
					// by other thread; b. this slot is changed.
					if (ret_cbitmap & (0x1ULL << slot))
					{
						inode->locker->ReadUnlock();
						return -1; // case b.
					}
					else
					{

						cbitmap = ret_cbitmap;
						continue; // case a. try again.
					}
				}

				// flush the commitbitmap and the fingerprints;
				pmemobj_persist(pop, &lfnode->commit_bitmap, 64);

				// insert has done.
				inode->locker->ReadUnlock();

				return 0;
			}
		}
	}

	assert(wbitmap == GROUP_BITMAP_FULL);
	assert(inode->nKeys <= MAX_LEAF_CAPACITY);

	if (inode->nKeys == MAX_LEAF_CAPACITY)
	{
		// this leaf block is full.
		if (!TryToGetWriteLock(inode, inode->is_split))
		{
			return +1; // other thread got the write lock.
		}
		inode->locker->AssertWriteHeld();

		// got write lock, split this inner node.
		{
			// split inner node.
			// fprintf(stderr, "split inner node!\n");

			////////////////////////////////////////////////////////////////////////////////////////////////
			// step 1 : create new inner node, set the next pointer and the max key and the slot pointer.
			////////////////////////////////////////////////////////////////////////////////////////////////
			ISN *new_in = create_inner_node(0);

			new_in->max_key = inode->max_key;
			new_in->next[0] = inode->next[0];
			new_in->nKeys = MAX_LEAF_CAPACITY - MIN_LEAF_CAPACITY;
			memcpy(&new_in->keys, &(inode->keys[MIN_LEAF_CAPACITY]),
				   sizeof(uint64_t) * new_in->nKeys);
			memcpy(&new_in->leaves, &(inode->leaves[MIN_LEAF_CAPACITY]),
				   sizeof(LSG *) * new_in->nKeys);
			memcpy(&new_in->mem_bitmap, &(inode->mem_bitmap[MIN_LEAF_CAPACITY]),
				   sizeof(uint64_t) * new_in->nKeys);
			// memset(&(inode->mem_bitmap[MIN_LEAF_CAPACITY]), 0, sizeof(uint64_t) * new_in->nKeys);
			// set the boundary
			new_in->leaves[0]->is_head = true;
			pmemobj_persist(pop, &new_in->leaves[0]->is_head, sizeof(bool));

			////////////////////////////////////////////////////////////////////////////////////////////////
			// step 2 : reset the old inner node's nKey and maxKey and next[0].
			////////////////////////////////////////////////////////////////////////////////////////////////
			inode->nKeys = MIN_LEAF_CAPACITY;
			inode->max_key = inode->keys[inode->nKeys - 1];
			inode->next[0] = new_in;
		}
		// leaf block split is done, release write lock.
		inode->is_split = false;
		inode->locker->WriteUnlock();
		// __atomic_store_n(&(inode->is_split), false, __ATOMIC_RELEASE);
		// #ifdef USE_AGG_KEYS
		// 		update_agg_keys(pre_nodes[MAX_L]);
		// #endif

		// insert again.
		return +99;
	}
	else
	{
		// this leaf node is full.
		if (!TryToGetWriteLock(inode, inode->is_split))
		{
			return +2; // other thread got the write lock.
		}
		inode->locker->AssertWriteHeld();

		// got write lock, split this leaf node.
		{
			assert(lfnode->commit_bitmap == GROUP_BITMAP_FULL);
			int group_idx[MAX_ENTRY_NUM], mid_idx = (MAX_ENTRY_NUM / 2);
			for (int i = 0; i < MAX_ENTRY_NUM; ++i)
			{
				group_idx[i] = i;
			}

			// partition sort the index of entries.
			quick_select_index(lfnode->entries, group_idx, mid_idx,
							   0, MAX_ENTRY_NUM - 1);

			// find the largest key in the left part.
			uint64_t left_largest = lfnode->entries[group_idx[0]].key;
			for (int i = 1; i < mid_idx; ++i)
			{
				if (lfnode->entries[group_idx[i]].key > left_largest)
				{
					left_largest = lfnode->entries[group_idx[i]].key;
				}
			}

			////////////////////////////////////////////////////////////////////////////////////////////////
			// step 1 : create a new leaf node, and set the flag , the next , the bitmap and fingerprints.
			////////////////////////////////////////////////////////////////////////////////////////////////
			LSG *new_slot = AllocNewLeafNode();
			new_slot->next = lfnode->next;
			// insert the last half entries to the new leaf node.
			int new_child_loc_slot = 0;
			uint64_t new_slot_bitmap = 0;
			for (int i = mid_idx; i < MAX_ENTRY_NUM; ++i, ++new_child_loc_slot)
			{
				new_slot->entries[new_child_loc_slot].key =
					lfnode->entries[group_idx[i]].key;
				new_slot->entries[new_child_loc_slot].value =
					lfnode->entries[group_idx[i]].value;
				new_slot->fingerprints[new_child_loc_slot] =
					lfnode->fingerprints[group_idx[i]];
				new_slot_bitmap |= (1ULL << new_child_loc_slot);
			}
			// change new leaf node's bitmap and maxkey.
			new_slot->commit_bitmap = new_slot_bitmap;
			// new_slot->working_bitmap = new_slot_bitmap;
			new_slot->max_key = lfnode->max_key;
			// flush the new leaf node.
			pmemobj_persist(pop, &new_slot, 128 + 16 * new_child_loc_slot); // 2 cache line size + key-value size

			////////////////////////////////////////////////////////////////////////////////////////////////
			// step 2 : change the slot's next pointer to new slot.
			////////////////////////////////////////////////////////////////////////////////////////////////
			lfnode->next = new_slot;
			pmemobj_persist(pop, &lfnode->next, sizeof(LSG *));

			////////////////////////////////////////////////////////////////////////////////////////////////
			// step 3 : reset the slot's bitmap.
			////////////////////////////////////////////////////////////////////////////////////////////////
			new_slot_bitmap = 0;
			for (int i = 0; i < mid_idx; ++i)
			{
				new_slot_bitmap |= (1ULL << (group_idx[i]));
			}
			lfnode->commit_bitmap = new_slot_bitmap;
			// lfnode->working_bitmap = new_slot_bitmap;
			// flush the old slot's commit bitmap.
			pmemobj_persist(pop, &lfnode->commit_bitmap, 8);

			////////////////////////////////////////////////////////////////////////////////////////////////
			// step 4 : change the old slot's max_key.
			////////////////////////////////////////////////////////////////////////////////////////////////
			lfnode->max_key = left_largest;
			pmemobj_persist(pop, &lfnode->max_key, 8);

			////////////////////////////////////////////////////////////////////////////////////////////////
			// step 5 : move inner node's max key and slot pointer to keep order.
			////////////////////////////////////////////////////////////////////////////////////////////////
			for (int i = inode->nKeys; i > loc + 1; --i)
			{
				inode->leaves[i] = inode->leaves[i - 1];
				// inode->keys[i] = inode->keys[i - 1];
				inode->mem_bitmap[i] = inode->leaves[i - 1]->commit_bitmap;
			}
			_mm_sfence();
			for (int i = inode->nKeys; i > loc + 1; --i)
			{
				// inode->leaves[i] = inode->leaves[i - 1];
				inode->keys[i] = inode->keys[i - 1];
				// inode->mem_bitmap[i] = inode->leaves[i - 1]->commit_bitmap;
			}
			inode->keys[loc + 1] = inode->keys[loc];
			inode->leaves[loc + 1] = new_slot;
			inode->mem_bitmap[loc + 1] = new_slot->commit_bitmap;
			inode->keys[loc] = left_largest;
			inode->mem_bitmap[loc] = lfnode->commit_bitmap;
			__atomic_add_fetch(&(inode->nKeys), 1, __ATOMIC_RELEASE);
		}
		// leaf node split is done, release write lock.
		inode->is_split = false;
		inode->locker->WriteUnlock();
		// __atomic_store_n(&(inode->is_split), false, __ATOMIC_RELEASE);

#ifdef PERF_PROFILING_W
		hist_set->Add(DO_SPLIT_LAEF, ElapsedNanos(t1));
#endif
		// insert again.
		return +99;
	}
}

uint64_t SearchINode(ISN *inode, uint64_t key)
{

	const uint8_t fp = f_hash(key);

	// May the leaf node we get is not the target leaf node, but the target leaf node must behind this leaf node.
	int child_loc = seq_search(inode, key);
	LSG *lfnode = inode->leaves[child_loc];
	if (lfnode == NULL)
	{
		printf("something wrong 1!\n");
		return 0;
	}
	uint64_t mLKey;
	uint64_t result = 0;
	while (true)
	{
		// mLKey = lfnode->max_key;
		mLKey = __atomic_load_n(&lfnode->max_key, __ATOMIC_CONSUME);
		while (mLKey < key)
		{
			// lfnode = lfnode->next;
			lfnode = __atomic_load_n(&lfnode->next, __ATOMIC_CONSUME);
			if (lfnode == NULL)
			{
				printf("something wrong 2!\n");
				return 0;
			}
			// mLKey = lfnode->max_key;
			mLKey = __atomic_load_n(&lfnode->max_key, __ATOMIC_CONSUME);
		}
		// At this moment, this maxkey and this lfnode is right.

		// probe bitmap one by one.
		const uint64_t bitmap = lfnode->commit_bitmap;
		for (int i = 0; i < MAX_ENTRY_NUM; ++i)
		{
			if ((bitmap & (0x1ULL << i)) && (lfnode->fingerprints[i] == fp) && (lfnode->entries[i].key == key))
			{
				result = lfnode->entries[i].value;
				break;
			}
		}

		if (inode->is_split || mLKey != lfnode->max_key)
		{
			continue;
		}
		break;
	}
	return result;
}

bool Insert(PHAST *list, uint64_t key, uint64_t value)
{
	int ret = 0;
	// [MAX_L] is assigned for the head.
	ISN *pre_nodes[MAX_L + 1], *next_nodes[MAX_L + 1], *target = NULL;

whole_retry:
	// search the target inner node first.
	target = SearchList(list->inner_list, key, pre_nodes, next_nodes);

	// we have assigned a inner node for each head.
	assert(target != NULL && !target->is_head && (target == pre_nodes[0]));

	target->locker->AssertReadHeld();

	ret = InsertIntoINode(target, key, value, pre_nodes, next_nodes);
	if (ret == 0)
	{
		return true;
	}
	else if (ret < 0)
	{
		return false;
	}
	else
	{
		if (ret == 1)
		{
			usleep(5); // sleep 5us if is splitting leaf block.
		}
		else if (ret == 2)
		{
			usleep(1); // sleep 1us if is splitting leaf block.
		}
		goto whole_retry;
	}
}

uint64_t Search(PHAST *list, uint64_t key)
{
	ISN *target = NULL;
	uint64_t ret = 0, target_maxkey;

	// search the target inner node first.
	target = SearchList(list->inner_list, key, &target_maxkey);

	// we have assigned a inner node for each head, so the target
	// cannot be a head.
	assert(target != NULL && !target->is_head);
	// target->locker->AssertReadHeld();

	return SearchINode(target, key);
}

int randomLevel()
{
	int level = 0;
	float f = 0.5 * 0xFFFF;
	while ((rand() & 0xFFFF) < f)
		level++;
	return (level < MAX_L) ? level : MAX_L - 1;
}

#ifdef USE_AGG_KEYS
void update_agg_keys(ISN *head, const int head_idx)
{
	assert(head->is_head);
	assert(head->agg_index != NULL);

	AGGIndex *new_idx = new AGGIndex(head, head->agg_index->NewSize());
	__atomic_store_n(&(head->agg_index), new_idx, __ATOMIC_RELEASE);
}

ISN *find_in_agg_keys(ISN *head, const uint64_t key, uint64_t *target_maxkey)
{
	assert(head->is_head);
	assert(head->agg_index != NULL);

	AGGIndex *old_idx = head->agg_index;

	return old_idx->Find(key, target_maxkey);
}
#endif

void free_inner_list(PHAST *list)
{
	ISL *inner_list = list->inner_list;

	ISN *q = inner_list->head[0];
	ISN *next = NULL;
	while (q)
	{
		next = q->next[0];
		free(q);
		q = next;
	}
	free(inner_list);
}

void ISN_free(ISN *innernode)
{
#ifdef USE_AGG_KEYS
	free(innernode->agg_index);
#endif
	free(innernode->locker);
	free(innernode);
}

void dram_free(PHAST *list)
{
	if (!list)
		return;
	ISL *inner_list = list->inner_list;
	ISN *q, *next;

	// free the inner node
	q = inner_list->head[0];
	while (q)
	{
		next = q->next[0];
		ISN_free(q);
		q = next;
	}
	free(inner_list);
	free(list);
}

PHAST *recovery(int n_threads)
{
	///////////////////////////
	// create new PHAST.
	///////////////////////////
	PHAST *phast = new PHAST;
	phast->inner_list = new InnerSkipList;
	InnerSkipList *list = phast->inner_list;

	///////////////////////////
	// init multiple header.
	///////////////////////////
	ISN *head = NULL;
	for (int i = 0; i < HEAD_COUNT; i++)
	{
		head = create_inner_node(0);
		if (head == NULL)
		{
			fprintf(stderr, "Memory allocation failed for head!");
			free(list);
			return NULL;
		}
		head->is_head = true;
		list->head[i] = head;
		list->level[i] = 0;
		// head1 [0] -> head2 [0] -> ...headx[0]-> NULL ;
		if (i > 0)
			for (int j = 0; j < MAX_L; j++)
				list->head[i - 1]->next[j] = list->head[i];
	}

	TOID(SHA)
	root = POBJ_ROOT(pop, SHA);
	LSG **head_slot_array = D_RW(root)->slot_head_array;

	///////////////////////////
	// Multithreading
	//////////////////////////
	std::vector<std::future<void>> futures(n_threads);
	uint64_t head_per_thread = HEAD_COUNT / n_threads;

	for (int tid = 0; tid < n_threads; tid++)
	{
		int from = head_per_thread * tid;
		int to = (tid == n_threads - 1) ? HEAD_COUNT : from + head_per_thread;

		auto f = std::async(
			std::launch::async,
			[&list, &head_slot_array](int from, int to)
			{
				for (int i = from; i < to; ++i)
				{ // loop:head
					ISN *head = list->head[i];
					ISN *cur_inode = head;

					LSG *head_slot = head_slot_array[i];
					LSG *pre_slot = NULL;
					LSG *cur_slot = head_slot;

					ISN *pre_inode[MAX_L];
					for (int i = 0; i < MAX_L; i++)
						pre_inode[i] = head;

					uint64_t pre_maxkey = 0;
					uint64_t count_pnode = 0;
					uint64_t key_boundary = (i == HEAD_COUNT - 1) ? MAX_U64_KEY : (i + 1) * HASH_KEY;
					while (cur_slot && cur_slot->max_key <= key_boundary)
					{ // loop:slot
						////////////////////////////////////////////////////////////////////////////
						// step 1: recalculate the fp;
						////////////////////////////////////////////////////////////////////////////
						uint64_t bitmap = cur_slot->commit_bitmap;
						// if (cur_slot->working_bitmap != bitmap)
						// 	cur_slot->working_bitmap = bitmap;
						for (int j = 0; j < MAX_ENTRY_NUM; j++)
							if ((bitmap & (0x1ULL << j)))
							{
								uint8_t fp = f_hash(cur_slot->entries[j].key);
								if (cur_slot->fingerprints[j] != fp)
									cur_slot->fingerprints[j] = fp;
							}

						////////////////////////////////////////////////////////////////////////////
						// step 2: determine if there are two identical max_keys
						////////////////////////////////////////////////////////////////////////////
						if (cur_slot->max_key == pre_maxkey)
						{
							// redo the slot split process. (1)reset the commit_bitmap.(2)update the maxkey(3)update innernode
							// assert(pre_slot->commit_bitmap == GROUP_BITMAP_FULL);
							pre_slot->commit_bitmap = ~cur_slot->commit_bitmap;
							// pre_slot->working_bitmap = pre_slot->commit_bitmap;
							pmemobj_persist(pop, &pre_slot->commit_bitmap, 8);

							uint64_t bitmap = pre_slot->commit_bitmap;
							uint64_t maxkey = 0;
							for (int j = 0; j < MAX_ENTRY_NUM; j++)
								if ((bitmap & (0x1ULL << j)) && pre_slot->entries[j].key > maxkey)
									maxkey = pre_slot->entries[j].key;

							assert(maxkey != 0);
							pre_slot->max_key = maxkey;
							pmemobj_persist(pop, &pre_slot->max_key, 8);

							cur_inode->keys[cur_inode->nKeys - 1] = maxkey;
							cur_inode->mem_bitmap[cur_inode->nKeys - 1] = pre_slot->commit_bitmap;
							cur_inode->max_key = maxkey;
						}

						////////////////////////////////////////////////////////////////////////////
						// step 3: add this slot to cur_inode;
						////////////////////////////////////////////////////////////////////////////
						if (cur_slot->is_head == true)
						{
							// create a new innernode and update the link.
							int level = randomLevel();
							if (level > list->level[i])
								list->level[i] = level;
							ISN *innode = create_inner_node(level);
							for (int j = 0; j <= level; j++)
							{
								pre_inode[j]->next[j] = innode;
								pre_inode[j] = innode;
							}
							count_pnode++;
							cur_inode = innode;
						}

						cur_inode->keys[cur_inode->nKeys] = cur_slot->max_key;
						cur_inode->mem_bitmap[cur_inode->nKeys] = cur_slot->commit_bitmap;
						cur_inode->leaves[cur_inode->nKeys] = cur_slot;
						cur_inode->max_key = cur_slot->max_key;
						cur_inode->nKeys++;

						////////////////////////////////////////////////////////////////////////////
						// step 4: update variables for the next loop;
						////////////////////////////////////////////////////////////////////////////
						pre_maxkey = cur_slot->max_key;
						pre_slot = cur_slot;
						cur_slot = cur_slot->next;
					}

// update the aggindex;
#ifdef USE_AGG_KEYS
					head->agg_index = new AGGIndex(head, count_pnode + AGG_REDUNDANT_SPACE);
#endif
				}
			},
			from, to);
		futures.push_back(move(f));
	}

	for (auto &&f : futures)
		if (f.valid())
			f.get();

	return phast;
}

uint64_t UpdateINode(ISN *inode, uint64_t key, uint64_t new_value)
{
	assert(inode->locker->AssertReadHeld());

	uint64_t old_value = 0;
	int threshold = 0;

	const uint8_t fp = f_hash(key);
	// search the target leaf node.
	const int child_loc = binary_search(inode, key);
	LSG *lfnode = inode->leaves[child_loc];
	if (UNLIKELY(lfnode == NULL))
	{
		return 0;
	}

	// probe bitmap one by one.
	const uint64_t bitmap = __atomic_load_n(&(lfnode->commit_bitmap), __ATOMIC_CONSUME);
	for (int i = 0; i < MAX_ENTRY_NUM; ++i)
	{
		if ((bitmap & (0x1ULL << i)) &&
			(lfnode->fingerprints[i] == fp) &&
			(lfnode->entries[i].key) == key)
		{
			old_value = lfnode->entries[i].value;
			// update the old value.
			lfnode->entries[i].value = new_value;
			pmemobj_persist(pop, &(lfnode->entries[i].value), 8);
			return old_value;
		}
	}

	return old_value;
}

uint64_t Update(PHAST *list, uint64_t key, uint64_t newValue)
{
	ISN *target = NULL;
	uint64_t ret = 0, target_maxkey;

	// search the target inner node first.
	target = SearchList(list->inner_list, key, &target_maxkey, true);

	// we have assigned a inner node for each head, so the target
	// cannot be a head.
	assert(target != NULL && !target->is_head);
	assert(target->locker->AssertReadHeld());

	ret = UpdateINode(target, key, newValue);
	target->locker->ReadUnlock();

	return ret;
}

int GetRangeFromSlot(LSG *slot, uint64_t start_key, Entry *candidate)
{
	// probe bitmap one by one.
	const uint64_t bitmap = __atomic_load_n(&(slot->commit_bitmap), __ATOMIC_CONSUME);
	int count = 0;
	if (start_key == 0)
	{
		for (int i = 0; i < MAX_ENTRY_NUM; ++i)
		{
			if (bitmap & (0x1ULL << i))
			{
				candidate[count++] = slot->entries[i];
			}
		}
	}
	else
	{
		for (int i = 0; i < MAX_ENTRY_NUM; ++i)
		{
			if (bitmap & (0x1ULL << i) &&
				slot->entries[i].key >= start_key)
			{
				candidate[count++] = slot->entries[i];
			}
		}
	}
	return count;
}

int Range_Search(PHAST *list, uint64_t key, int num, uint64_t *buf)
{
	ISN *target = NULL;
	uint64_t target_maxkey;

	// search the target inner node first.
	target = SearchList(list->inner_list, key, &target_maxkey);

	// we have assigned a inner node for each head, so the target
	// cannot be a head.
	assert(target != NULL && !target->is_head);

	////////////////////////////////////////
	// 1. get the right slot.
	//    Because slot is never free, it must be the one even moved to another node.
	////////////////////////////////////////
	int child_loc = binary_search(target, key);
	LSG *lfnode = target->leaves[child_loc];
	while (true)
	{
		if (UNLIKELY(__atomic_load_n(&(target->max_key), __ATOMIC_CONSUME) < key))
		{
			// target has split and the range has changed.
			target = __atomic_load_n(&(target->next[0]), __ATOMIC_CONSUME);
			while (target != NULL && target->is_head)
			{
				// skip head node.
				target = __atomic_load_n(&(target->next[0]), __ATOMIC_CONSUME);
			}
			if (UNLIKELY(target == NULL || target->is_head))
			{
				// reach the tail of the skiplist || abort.
				return 0;
			}

			child_loc = binary_search(target, key);
			lfnode = target->leaves[child_loc];
			continue;
		}
		break;
	}

	if (lfnode == NULL)
	{
		// abort!
		for_debug();
		assert(false);
		return 0;
	}

	////////////////////////////////////////
	// 2. get values from the slot.
	////////////////////////////////////////
	Entry candidate[num + MAX_ENTRY_NUM];
	int got_count = 0; // no. elements in candidate.
	int xnum = 0;	   // no. entries got from one slot.
	uint64_t low_key = key;
	while (lfnode != NULL && got_count < num)
	{
		LSG *lf_next = lfnode->next;
		xnum = GetRangeFromSlot(lfnode, low_key, &(candidate[got_count]));
		if (lf_next != __atomic_load_n(&(lfnode->next), __ATOMIC_CONSUME))
		{
			// this lfnode has been split, re-scan this slot to avoid double refs.
			continue;
		}
		else
		{
			lfnode = lf_next;
		}

		// lfnode has been reset as the next, if the scanned one is splitting,
		// it doesnot matter due to we has skipped it.
		got_count += xnum;
		if (got_count > num)
		{
			// we have got the full keys, just break from the while loop.
			break;
		}

		// reset start_key to indicate no compare when get keys from the next slot.
		low_key = 0;
		// sort the got keys.
		insertion_sort_entry(&(candidate[got_count - xnum]), xnum);
	}

	////////////////////////////////////////
	// 3. check sort status and keys got from the last scan.
	////////////////////////////////////////
	int ret_count = (got_count > num) ? num : got_count;
	if (got_count > num)
	{
		// partition the keys got from the last scan to avoid
		// sort all keys.
		quick_select(candidate, num, got_count - xnum, got_count - 1);

		// sort the first part of the keys.
		insertion_sort_entry(&(candidate[got_count - xnum]), num - (got_count - xnum));
	}

	// copy value.
	for (int i = 0; i < ret_count; ++i)
	{
		buf[i] = candidate[i].value;
	}
	return ret_count;
}

uint64_t Delete(PHAST *list, uint64_t key)
{
	return Update(list, key, MAX_U64_KEY);
}

void print_list_all(PHAST *list, uint64_t key)
{
	int head_idx = key / HASH_KEY;
	ISN *header = list->inner_list->head[head_idx];
	print_list_all(header);
}

void print_list_all(PHAST *list)
{
	for (int i = 0; i < HEAD_COUNT; ++i)
	{
		print_list_all(list->inner_list->head[i]);
	}
}

void print_list_all(ISN *header)
{
	assert(header->is_head);
	ISN *node = header->next[0];
	int pos = 0;
	while (node != NULL && !(node->is_head))
	{
		fprintf(stderr, "node[%d]: max key: %llu, level: %d, nKeys: %d\n",
				pos++, node->max_key, node->nLevel, node->nKeys);
		for (int i = 0; i < node->nKeys; ++i)
		{
			fprintf(stderr, "%llu, ", node->keys[i]);
		}
		fprintf(stderr, "\n");
		node = node->next[0];
	}
}

void print_inode_and_next(ISN *node)
{
	int pos = 0;
	ISN *next = node->next[0];

	fprintf(stderr, "node[%d]: max key: %llu, level: %d, nKeys: %d\n",
			pos++, node->max_key, node->nLevel, node->nKeys);
	for (int i = 0; i < node->nKeys; ++i)
	{
		fprintf(stderr, "%llu, ", node->keys[i]);
	}
	fprintf(stderr, "\n");

	if (next != NULL && !(next->is_head))
	{
		fprintf(stderr, "node[%d]: max key: %llu, level: %d, nKeys: %d\n",
				pos++, node->max_key, node->nLevel, node->nKeys);
		for (int i = 0; i < node->nKeys; ++i)
		{
			fprintf(stderr, "%llu, ", node->keys[i]);
		}
		fprintf(stderr, "\n");
	}
}

void print_lnode_all(LSG *node, uint64_t maxkey, const uint64_t bitmap)
{
	fprintf(stderr, "--> pre_maxkey: %lu, max key: %lu\n", maxkey, node->max_key);
	for (int i = 0; i < MAX_ENTRY_NUM; ++i)
	{
		if ((bitmap & (0x1ULL << i)))
		{
			fprintf(stderr, "\t#%d %lu %lu\n", i, node->entries[i].key, node->entries[i].value);
		}
	}
}

void print_lnode_all(LSG *node)
{
	fprintf(stderr, "--> max key: %lu\n", node->max_key);
	const uint64_t bitmap = __atomic_load_n(&(node->commit_bitmap), __ATOMIC_CONSUME);
	for (int i = 0; i < MAX_ENTRY_NUM; ++i)
	{
		if ((bitmap & (0x1ULL << i)))
		{
			fprintf(stderr, "\t#%d %lu %lu\n", i, node->entries[i].key, node->entries[i].value);
		}
	}
}

void print_lnode_and_next(LSG *node)
{
	print_lnode_all(node);
	LSG *next = node->next;
	if (next != NULL)
	{
		print_lnode_all(next);
	}
}

void print_list_skeleton(PHAST *list)
{
	// for (int i = 0; i < HEAD_COUNT; ++i) {
	print_list_skeleton(list->inner_list->head[0]);
	// }
}

void print_list_skeleton(ISN *header)
{
	size_t level_nodes[MAX_L];
	ISN *header_nodes[MAX_L];

	for (int level = 0; level < MAX_L; ++level)
	{
		level_nodes[level] = 0;
		header_nodes[level] = header->next[level];
	}

	for (int level = 0; level < MAX_L; ++level)
	{
		ISN *node = header_nodes[level];
		while (node != NULL)
		{
			++level_nodes[level];
			node = node->next[level];
		}
	}

	for (int level = MAX_L - 1; level >= 0; --level)
	{
		printf("Level: %2d has %zu nodes\n", level + 1, level_nodes[level]);
	}
}
/*
void print_mem_nvm_comsumption(PHAST *list) {
	uint64_t in_num = 0, hd_num = 0, lb_num = 0;
	uint64_t agg_size = 0, mem_size = 0, nvmm_size = 0;

#ifdef USE_AGG_KEYS
	agg_size = sizeof(int) * HEAD_COUNT * 2;
	for (int i = 0; i < HEAD_COUNT; ++i) {
		agg_size += sizeof(uint64_t) * list->inner_list->head[i]->agg_index->Cap();
		agg_size += sizeof(ISN*) * list->inner_list->head[i]->agg_index->Cap();
	}
#endif

#ifdef PERF_PROFILING_M
	hist_set->Clear(ELE_IN_LB);
	hist_set->Clear(ELE_IN_LN);
#endif

	ISN *node = list->inner_list->head[0];
	while (node != NULL) {
		ISN *tmp = node;
		node = node->next[0];
		if (tmp->is_head) {
			++hd_num;
			continue;
		}

		++in_num;
		if (tmp->leaf_node != NULL) {
			++lb_num;
		}

#ifdef PERF_PROFILING_M
		int lb_load = 0, ln_load = 0;
		for (int i = 0; i < MAX_LEAF_CAPACITY / BITMAP_SIZE; ++i) {
			lb_load += popcount1(tmp->leaf_node->bitmap_LN[i]);
		}
		for (int i = 0; i < MAX_LEAF_CAPACITY; ++i) {
			ln_load = popcount1(tmp->leaf_node->leaves[i].commit_bitmap);
			hist_set->Add(ELE_IN_LN, ln_load);
		}
		hist_set->Add(ELE_IN_LB, lb_load);
#endif
	}

#ifdef PERF_PROFILING_M
	hist_set->PrintResult(ELE_IN_LB);
	hist_set->PrintResult(ELE_IN_LN);
	hist_set->Clear(ELE_IN_LB);
	hist_set->Clear(ELE_IN_LN);
#endif

	mem_size += agg_size;
	mem_size += (hd_num + in_num) * sizeof(ISN);
	nvmm_size += lb_num * sizeof(LSN);

	fprintf(stderr, "Memory consumption: %lu bytes\n", mem_size);
	fprintf(stderr, "NVMM consumption: %lu bytes\n", nvmm_size);
}
*/
