#include "PHAST.h"

#define INSERT_NUM (5000000)     // shared by threads.
#define SEARCH_NUM (5000000)     // shared by threads.
#define MIX_INSERT_NUM (5000000) // shared by threads.
#define MIX_SEARCH_NUM (5000000) // shared by threads.

#define TEST_SCAN true     // wether to test scan ,update and delete.
#define TEST_RECOVERY false // wether to test recovery.
// #define MIXED_WORKLOAD
#define CHECK_AFTER_OPS

#define SEQ_KEYS_ORDER false

void clear_cache()
{
    // Remove cache
    int size = 256 * 1024 * 1024;
    char *garbage = new char[size];
    for (int i = 0; i < size; ++i)
        garbage[i] = i;
    for (int i = 100; i < size; ++i)
        garbage[i] += garbage[i - 100];
    delete[] garbage;
}

void preformace_test(int n_threads, int num = 0)
{
    fprintf(stderr, "/////////////////////////////////////////\n");
    // fprintf(stderr, "the number of Agglevel is %d\n",AGG_UPDATE_LEVEL);
    fprintf(stderr, "PHAST(partition) : n_thread is: %d\n", n_threads);
    PHAST *list = init_list();
    assert(list != NULL);

    if (num == 0)
    {
        num = (INSERT_NUM + SEARCH_NUM);
    }
    uint64_t t1;

#ifdef CHECK_AFTER_OPS
    uint64_t chk_num = 0;
    uint64_t chk_num_not_find = 0;
#endif

    //////////////////////////
    // generate keys.
    /////////////////////////
    uint64_t *keys = (uint64_t *)malloc(num * sizeof(uint64_t));
    std::random_device rd;
    std::mt19937_64 eng(rd());
#if 0
    std::uniform_int_distribution<unsigned long long> uniform_dist;
    for (uint64_t i = 0; i < num;)
    {
        uint64_t x = uniform_dist(eng);
        if (x > 0 && x < MAX_U64_KEY)
        {
           keys[i++] = x;
        //    keys[i++] = i;
        }
        else
        {
            continue;
        }
    }
#else
    uint64_t step = MAX_U64_KEY / num;
    for (uint64_t i = 0; i < num; i++)
        keys[i] = i * step + 1;
    if (!SEQ_KEYS_ORDER)
        std::shuffle(keys, keys + num, eng);
#endif

    ///////////////////////////
    //-----Warm up-----
    ///////////////////////////
    t1 = NowNanos();
    fprintf(stderr, "single thread start warm up!\n");
    for (uint64_t i = 0; i < num / 2; ++i)
    {
        Insert(list, keys[i], keys[i]);
    }

    // fprintf(stderr, "single thread finish warm up! %llu ns.\n", ElapsedNanos(t1));

    ///////////////////////////
    // Multithreading
    //////////////////////////
    std::vector<std::future<void>> futures(n_threads);
    uint64_t data_per_thread = (num / 2) / n_threads;

#ifndef MIXED_WORKLOAD
    ///////////////////////////
    //-----Test Insert-----
    ///////////////////////////
    fprintf(stderr, "/////////////////////////////////////////\n");
    fprintf(stderr, "%d threads start insert\n", n_threads);
    clear_cache();
    size_t seq_cursor = (num / 2) - 1; // add_and_fetch is a little faster than fetch_and_add.
    t1 = NowNanos();

    for (int tid = 0; tid < n_threads; tid++)
    {
        int from = data_per_thread * tid;
        int to = (tid == n_threads - 1) ? num / 2 : from + data_per_thread;

        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num, &seq_cursor](int from, int to, int tid)
            {
                if (SEQ_KEYS_ORDER)
                {
                    size_t i = __sync_add_and_fetch(&seq_cursor, 1);
                    while (i < num)
                    {
                        Insert(list, keys[i], keys[i]);
                        i = __sync_add_and_fetch(&seq_cursor, 1);
                    }
                }
                else
                {
                    for (int i = from + num / 2; i < to + num / 2; ++i)
                        Insert(list, keys[i], keys[i]);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    fprintf(stderr, "%d threads insert time cost is %llu ns.\n", n_threads, ElapsedNanos(t1));


    /////////////////////////////
    //-----Test Search-----
    ////////////////////////////
    fprintf(stderr, "/////////////////////////////////////////\n");
    fprintf(stderr, "%d threads start search\n", n_threads);
    clear_cache();
    futures.clear();
    t1 = NowNanos();
#ifdef CHECK_AFTER_OPS
    chk_num = 0;
#endif

    for (int tid = 0; tid < n_threads; tid++)
    {
        int from = data_per_thread * tid;
        int to = (tid == n_threads - 1) ? num / 2 : from + data_per_thread;

#ifdef CHECK_AFTER_OPS
        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num, &chk_num](int from, int to, int tid)
            {
                for (int i = from + num / 2; i < to + num / 2; ++i)
                {
                    if (Search(list, keys[i]) != keys[i])
                    {
                        __sync_fetch_and_add(&chk_num, 1);
                    }
                }
            },
            from, to, tid);
#else
        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num](int from, int to, int tid)
            {
                for (int i = from + num / 2; i < to + num / 2; ++i)
                    Search(list, keys[i]);
            },
            from, to, tid);
#endif
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    fprintf(stderr, "%d threads search time cost is %llu ns.\n", n_threads, ElapsedNanos(t1));
#ifdef CHECK_AFTER_OPS
    fprintf(stderr, "%llu/%llu (%.2f%%) wrong search results\n", chk_num, num / 2,
            ((float)chk_num) / (float)(num / 2));
#endif

#else
    /////////////////////////////
    //-----Test Mixed-----
    ////////////////////////////
    fprintf(stderr, "/////////////////////////////////////////\n");
    fprintf(stderr, "start mixed!\n");
    clear_cache();
    futures.clear();
    uint64_t half_num_data = num / 2;
    size_t seq_cursor = (num / 2) - 1; // add_and_fetch is a little faster than fetch_and_add.
    t1 = NowNanos();
#ifdef CHECK_AFTER_OPS
    chk_num = 0;
#endif

    for (int tid = 0; tid < n_threads; tid++)
    {
        int from = half_num_data + data_per_thread * tid;
        int to = (tid == n_threads - 1) ? num : from + data_per_thread;

#ifdef CHECK_AFTER_OPS
        auto f = std::async(
            std::launch::async,
            [&list, &keys, &half_num_data, &chk_num, &chk_num_not_find](int from, int to, int tid)
            {
                for (int i = from; i < to; ++i)
                {
                    int sidx = i - half_num_data;
                    int jid = i % 4;
                    uint64_t res = 0;
                    switch (jid)
                    {
                    case 0:
                        Insert(list, keys[i], keys[i]);
                        for (int j = 0; j < 4; j++)
                        {
                            res = Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            if (res != keys[(sidx + j + jid * 8) % half_num_data])
                            {
                                __sync_fetch_and_add(&chk_num, 1);
                            }
                            if (res == 0)
                            {
                                __sync_fetch_and_add(&chk_num_not_find, 1);
                            }
                        }
                        break;
                    case 1:
                        for (int j = 0; j < 3; j++)
                        {
                            res = Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            if (res != keys[(sidx + j + jid * 8) % half_num_data])
                            {
                                __sync_fetch_and_add(&chk_num, 1);
                            }
                            if (res == 0)
                            {
                                __sync_fetch_and_add(&chk_num_not_find, 1);
                            }
                        }
                        Insert(list, keys[i], keys[i]);
                        res = Search(list, keys[(sidx + 3 + jid * 8) % half_num_data]);
                        if (res != keys[(sidx + 3 + jid * 8) % half_num_data])
                        {
                            __sync_fetch_and_add(&chk_num, 1);
                        }
                        if (res == 0)
                        {
                            __sync_fetch_and_add(&chk_num_not_find, 1);
                        }
                        break;
                    case 2:
                        for (int j = 0; j < 2; j++)
                        {
                            res = Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            if (res != keys[(sidx + j + jid * 8) % half_num_data])
                            {
                                __sync_fetch_and_add(&chk_num, 1);
                            }
                            if (res == 0)
                            {
                                __sync_fetch_and_add(&chk_num_not_find, 1);
                            }
                        }
                        Insert(list, keys[i], keys[i]);
                        for (int j = 2; j < 4; j++)
                        {
                            res = Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            if (res !=
                                keys[(sidx + j + jid * 8) % half_num_data])
                            {
                                __sync_fetch_and_add(&chk_num, 1);
                            }
                            if (res == 0)
                            {
                                __sync_fetch_and_add(&chk_num_not_find, 1);
                            }
                        }
                        break;
                    case 3:
                        for (int j = 0; j < 4; j++)
                        {
                            res = Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            if (res !=
                                keys[(sidx + j + jid * 8) % half_num_data])
                            {
                                __sync_fetch_and_add(&chk_num, 1);
                            }
                            if (res == 0)
                            {
                                __sync_fetch_and_add(&chk_num_not_find, 1);
                            }
                        }
                        Insert(list, keys[i], keys[i]);
                        break;
                    default:
                        break;
                    }
                }
            },
            from, to, tid);
#else
        auto f = std::async(
            std::launch::async,
            [&list, &keys, &half_num_data, &num, &seq_cursor](int from, int to, int tid)
            {
                if (SEQ_KEYS_ORDER)
                {
                    size_t i = __sync_add_and_fetch(&seq_cursor, 1);
                    while (i < num)
                    {
                        int jid = i % 4;
                        switch (jid)
                        {
                        case 0:
                            Insert(list, keys[i], keys[i]);
                            for (int j = 0; j < 4; j++)
                            {
                                Search(list, keys[(i - j - jid * 8)]);
                            }
                            break;
                        case 1:
                            for (int j = 0; j < 3; j++)
                            {
                                Search(list, keys[(i - j - jid * 8)]);
                            }
                            Insert(list, keys[i], keys[i]);
                            Search(list, keys[(i - 3 - jid * 8)]);
                            break;
                        case 2:
                            for (int j = 0; j < 2; j++)
                            {
                                Search(list, keys[(i - j - jid * 8)]);
                            }
                            Insert(list, keys[i], keys[i]);
                            for (int j = 2; j < 4; j++)
                            {
                                Search(list, keys[(i - j - jid * 8)]);
                            }
                            break;
                        case 3:
                            for (int j = 0; j < 4; j++)
                            {
                                Search(list, keys[(i - j - jid * 8)]);
                            }
                            Insert(list, keys[i], keys[i]);
                            break;
                        default:
                            break;
                        }
                        i = __sync_add_and_fetch(&seq_cursor, 1);
                    }
                }
                else
                {
                    for (int i = from; i < to; ++i)
                    {
                        int sidx = i - half_num_data;

                        int jid = i % 4;
                        switch (jid)
                        {
                        case 0:
                            Insert(list, keys[i], keys[i]);
                            for (int j = 0; j < 4; j++)
                            {
                                Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            }
                            break;
                        case 1:
                            for (int j = 0; j < 3; j++)
                            {
                                Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            }
                            Insert(list, keys[i], keys[i]);
                            Search(list, keys[(sidx + 3 + jid * 8) % half_num_data]);
                            break;
                        case 2:
                            for (int j = 0; j < 2; j++)
                            {
                                Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            }
                            Insert(list, keys[i], keys[i]);
                            for (int j = 2; j < 4; j++)
                            {
                                Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            }
                            break;
                        case 3:
                            for (int j = 0; j < 4; j++)
                            {
                                Search(list, keys[(sidx + j + jid * 8) % half_num_data]);
                            }
                            Insert(list, keys[i], keys[i]);
                            break;
                        default:
                            break;
                        }
                    }
                }
            },
            from, to, tid);
#endif
        futures.push_back(move(f));
    }

    for (auto &&f : futures)
        if (f.valid())
            f.get();

    fprintf(stderr, "%d threads mixed time cost is %llu\n", n_threads, ElapsedNanos(t1));
#ifdef CHECK_AFTER_OPS
    fprintf(stderr, "%llu/%llu (%.2f%%) wrong search results\n", chk_num, num * 2,
            ((float)chk_num) / (float)(num * 2));
    fprintf(stderr, "%llu/%llu (%.2f%%) search not find\n", chk_num_not_find, num * 2,
            ((float)chk_num_not_find) / (float)(num * 2));
#endif
#endif

#if TEST_SCAN
    ///////////////////////////
    //-----Test Scan-----
    ///////////////////////////
    fprintf(stderr, "/////////////////////////////////////////\n");
    fprintf(stderr, "%d threads start scan\n", n_threads);
    clear_cache();
    t1 = NowNanos();

    for (int tid = 0; tid < n_threads; tid++)
    {
        uint64_t from = data_per_thread * tid;
        uint64_t to = (tid == n_threads - 1) ? num / 2 : from + data_per_thread;

        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num](uint64_t from, uint64_t to, int tid)
            {
                uint64_t scan_buf[51];
                for (uint64_t i = from + num / 2; i < to + num / 2; ++i)
                {
                    Range_Search(list, keys[i], 50, scan_buf);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    fprintf(stderr, "%d threads scan time cost is %llu ns.\n", n_threads, ElapsedNanos(t1));

    ///////////////////////////
    //-----Test Update-----
    ///////////////////////////
    fprintf(stderr, "/////////////////////////////////////////\n");
    fprintf(stderr, "%d threads start update\n", n_threads);
    clear_cache();
    t1 = NowNanos();

    for (int tid = 0; tid < n_threads; tid++)
    {
        uint64_t from = data_per_thread * tid;
        uint64_t to = (tid == n_threads - 1) ? num / 2 : from + data_per_thread;

        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num](uint64_t from, uint64_t to, int tid)
            {
                for (uint64_t i = from + num / 2; i < to + num / 2; ++i)
                {
                    Update(list, keys[i], keys[i] + 1);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    fprintf(stderr, "%d threads update time cost is %llu ns.\n", n_threads, ElapsedNanos(t1));

    ///////////////////////////
    //-----Test Delete-----
    ///////////////////////////
    fprintf(stderr, "/////////////////////////////////////////\n");
    fprintf(stderr, "%d threads start delete\n", n_threads);
    clear_cache();
    t1 = NowNanos();

    for (int tid = 0; tid < n_threads; tid++)
    {
        uint64_t from = data_per_thread * tid;
        uint64_t to = (tid == n_threads - 1) ? num / 2 : from + data_per_thread;

        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num](uint64_t from, uint64_t to, int tid)
            {
                for (uint64_t i = from + num / 2; i < to + num / 2; ++i)
                {
                    Delete(list, keys[i]);
                }
            },
            from, to, tid);
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    fprintf(stderr, "%d threads delete time cost is %llu ns.\n", n_threads, ElapsedNanos(t1));

#endif

#if TEST_RECOVERY
    ///////////////////////////////////////////
    //-----Test recovery-----
    //////////////////////////////////////////
    for (int i = 0; i < 6; i++)
    {
        int n_threads = std::pow(2, i);
        fprintf(stderr, "/////////////////////////////////////////\n");
        fprintf(stderr, "%d threads start recovery\n", n_threads);
        dram_free(list);
        clear_cache();
        t1 = NowNanos();
        list = recovery(n_threads);
        fprintf(stderr, "%d threads recovery time cost is %llu ns.\n", n_threads, ElapsedNanos(t1));
    }
    /////////////////////////////
    //-----Test Search(after recovery)-----
    ////////////////////////////
    fprintf(stderr, "/////////////////////////////////////////\n");
    fprintf(stderr, "%d threads start search\n", n_threads);
    clear_cache();
    futures.clear();
    t1 = NowNanos();
#ifdef CHECK_AFTER_OPS
    chk_num = 0;
#endif

    for (int tid = 0; tid < n_threads; tid++)
    {
        int from = data_per_thread * tid;
        int to = (tid == n_threads - 1) ? num / 2 : from + data_per_thread;

#ifdef CHECK_AFTER_OPS
        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num, &chk_num](int from, int to, int tid)
            {
                for (int i = from + num / 2; i < to + num / 2; ++i)
                {
                    if (Search(list, keys[i]) != keys[i])
                    {
                        __sync_fetch_and_add(&chk_num, 1);
                    }
                }
            },
            from, to, tid);
#else
        auto f = std::async(
            std::launch::async,
            [&list, &keys, &num](int from, int to, int tid)
            {
                for (int i = from + num / 2; i < to + num / 2; ++i)
                    Search(list, keys[i]);
            },
            from, to, tid);
#endif
        futures.push_back(move(f));
    }
    for (auto &&f : futures)
        if (f.valid())
            f.get();
    fprintf(stderr, "%d threads search time cost is %llu ns.\n", n_threads, ElapsedNanos(t1));
#ifdef CHECK_AFTER_OPS
    fprintf(stderr, "%llu/%llu (%.2f%%) wrong search results\n", chk_num, num / 2,
            ((float)chk_num) / (float)(num / 2));
#endif

#endif
    delete list;
    free(keys);
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "The parameter numThread is required!\n");
        return 0;
    }

    int num_thread = atoi(argv[1]);
    preformace_test(num_thread);
    return 0;
}
