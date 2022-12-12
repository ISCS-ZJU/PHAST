#pragma once

#include <time.h>
#include <sys/time.h>
#include <inttypes.h>
#include <vector>
#include <algorithm>
#include <string>
#include <fstream>
#include <iostream>

static inline uint64_t NowNanos() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

static inline uint64_t NowMicros() {
	struct timeval tv;
	gettimeofday(&tv, nullptr);
	return (uint64_t)(tv.tv_sec) * 1000000 + tv.tv_usec;
}

static inline uint64_t ElapsedNanos(uint64_t start_time) {
	uint64_t now = NowNanos();
	return now - start_time;
}

static inline uint64_t ElapsedMicros(uint64_t start_time) {
	uint64_t now = NowMicros();
	return now - start_time;
}

class Counter {
  public:
	explicit Counter(std::string name) : num_(0), name_(name) {};
	~Counter() {};

	void Clear() { num_ = 0; }

	void Add(uint64_t t) {
#ifndef THREAD_SAFE_TIMER
		num_ += t;
#else
		__sync_add_and_fetch(&num_, t);
#endif
	}

	void PrintResult() {
		fprintf(stderr, "%s: %lu\n", name_.c_str(), num_);
	}

  private:
	uint64_t num_;
	std::string name_;
};

class Histogram {
  public:
	explicit Histogram(std::string name) : name_(name) {
		finallized_ = false;
#ifdef THREAD_SAFE_TIMER
		pthread_mutex_init(&mu_, NULL);
#endif
	}
	~Histogram() {
#ifdef THREAD_SAFE_TIMER
		pthread_mutex_destroy(&mu_);
#endif
	}

	void Clear() { values_.clear(); }

	void Add(uint64_t t) {
#ifndef THREAD_SAFE_TIMER
		values_.push_back(t);
#else
		pthread_mutex_lock(&mu_);
		values_.push_back(t);
		pthread_mutex_unlock(&mu_);
#endif
	}

	void Finallize() {
		if (!finallized_) {
			std::sort(values_.begin(), values_.end());
			finallized_ = true;
		}
	}

	// call after Finallize.
	size_t ValizePos(size_t pos) {
		if (pos >= values_.size()) return values_.size() - 1;
	}

	double Min() {
		return (values_.empty()) ? 0 : (double)values_.front();
	}

	double Max() {
		return (values_.empty()) ? 0 : (double)values_.back();
	}

	double Sum() {
		double sum = 0;
		for (auto x : values_) {
			sum += x;
		}
		return sum;
	}

	double Avg() {
		if (values_.empty()) return 0;
		double sum = 0;
		for (auto x : values_) {
			sum += x;
		}
		return sum / values_.size();
	}

	double P50() {
		if (values_.empty()) return 0;
		size_t pos = values_.size() / 2;
		return (double)values_[pos];
	}

	double P99() {
		if (values_.empty()) return 0;
		size_t pos = values_.size() * 99 / 100;
		return (double)values_[pos];
	}

	double P995() {
		if (values_.empty()) return 0;
		size_t pos = values_.size() * 995 / 1000;
		return (double)values_[pos];
	}

	double P999() {
		if (values_.empty()) return 0;
		size_t pos = values_.size() * 999 / 1000;
		return (double)values_[pos];
	}

	double PXX(int x, int y) {
		if (values_.empty()) return 0;
		size_t pos = values_.size() * x / y;
		return (double)values_[pos];
	}

	void PrintResult() {
		if (values_.size() == 0) {
			fprintf(stderr, "%s: NO STAT\n", name_.c_str());
			return;
		}
		Finallize();
		fprintf(stderr, "%s:\n", name_.c_str());
		fprintf(stderr, "\tCount: %zu\n", values_.size());
		fprintf(stderr, "\tMin: %.6f\n", Min());
		fprintf(stderr, "\tp50: %.6f\n", P50());
		fprintf(stderr, "\tp99: %.6f\n", P99());
		fprintf(stderr, "\tp995: %.6f\n", P995());
		fprintf(stderr, "\tp999: %.6f\n", P999());
		fprintf(stderr, "\tMax: %.6f\n", Max());
		fprintf(stderr, "\tSum: %.6f\n", Sum());
	}

	void dump_to_file(std::string fname, size_t dump_num) {
		if (dump_num < 100) {
			fprintf(stderr, "%s() dump_num %zu is too small\n", __FUNCTION__, dump_num);
			return;
		}

		if (!finallized_) {
			Finallize();
		}

		if (dump_num > values_.size()) {
			dump_num = values_.size();
		}

		int i = 0;
		int step = values_.size() / dump_num;
		std::string buf;
		for (int i = 0; i < values_.size(); i += step) {
			buf.append(std::to_string(values_[i]));
			buf.append("\n");
		}

		// check the mas value.
		if (i - step != values_.size() - 1) {
			buf.append(std::to_string(values_[values_.size() - 1]));
			buf.append("\n");
		}

		// open file and write buf.
		std::ofstream fout(fname);

		fout << buf;
		fout.close();
	}

	std::vector<uint64_t> values_;
  private:
	bool finallized_;

	std::string name_;
#ifdef THREAD_SAFE_TIMER
	pthread_mutex_t mu_;
#endif
};

enum STATISTICS {
	DO_INSERT = 0,
	DO_SEARCH,
	DO_UPDATE,
	DO_SCAN,
	DO_DELETE,
	DO_SEARCH_LIST_1,
	DO_SEARCH_LIST_1_1,
	DO_SEARCH_LIST_1_2,
	DO_SEARCH_LIST_1_3,
	DO_SEARCH_LIST_2,
	DO_SEARCH_LIST_2_1,
	DO_SEARCH_LIST_2_2,
	DO_SEARCH_LIST_2_3,
	DO_SEARCH_BLOCK,
	DO_SEARCH_LEAF,
	DO_SEARCH_LEAF_PROBE,
	DO_INSERT_LEAF,
	DO_INSERT_LEAF_1,
	DO_INSERT_LEAF_2,
	DO_INSERT_LEAF_3,
	DO_INSERT_LEAF_4,
	DO_SPLIT_LAEF,
	DO_SPLIT_BLOCK,
	DO_UPDATE_AGG_KEYS,
	DO_SEARCH_AGG_KEYS,
	ELE_IN_LB,
	ELE_IN_LN,
	FLUSH_TIME,
	FFZ,

	MAX_NUM,
};

static std::string STATISTICS_STRING[] = {
	"DO_INSERT",
	"DO_SEARCH",
	"DO_UPDATE",
	"DO_SCAN",
	"DO_DELETE",
	"DO_SEARCH_LIST_1",
	"DO_SEARCH_LIST_1_1",
	"DO_SEARCH_LIST_1_2",
	"DO_SEARCH_LIST_1_3",
	"DO_SEARCH_LIST_2",
	"DO_SEARCH_LIST_2_1",
	"DO_SEARCH_LIST_2_2",
	"DO_SEARCH_LIST_2_3",
	"DO_SEARCH_BLOCK",
	"DO_SEARCH_LEAF",
	"DO_SEARCH_LEAF_PROBE",
	"DO_INSERT_LEAF",
	"DO_INSERT_LEAF_1",
	"DO_INSERT_LEAF_2",
	"DO_INSERT_LEAF_3",
	"DO_INSERT_LEAF_4",
	"DO_SPLIT_LAEF",
	"DO_SPLIT_BLOCK",
	"DO_UPDATE_AGG_KEYS",
	"DO_SEARCH_AGG_KEYS",
	"ELE_IN_LB",
	"ELE_IN_LN",
	"FLUSH_TIME",
	"FFZ",

	"MAX_NUM"
};

class HistogramSet {
  public:
	HistogramSet() {
		for (int i = 0; i < STATISTICS::MAX_NUM; ++i) {
			hist_set_.emplace_back(new Histogram(STATISTICS_STRING[i]));
		}
	}

	~HistogramSet() {
		for (auto x : hist_set_) {
			delete x;
		}
	}

	void Clear(size_t pos) {
		hist_set_[(size_t)pos]->Clear();
	}

	void Clear() {
		for (auto x : hist_set_) {
			x->Clear();
		}
	}

	void AddNewHist(std::string name) {
		hist_set_.emplace_back(new Histogram(name));
	}

	void Add(size_t pos, uint64_t t) {
		hist_set_[(size_t)pos]->Add(t);
	}

	void PrintResult(size_t pos) {
		hist_set_[(size_t)pos]->PrintResult();
	}

	void PrintResult() {
		for (size_t i = 0; i < hist_set_.size(); ++i) {
			// fprintf(stderr, "%s:\n", STATISTICS_STRING[i].c_str());
			hist_set_[i]->PrintResult();
			fprintf(stderr, "\n");
		}
	}


  private:
	std::vector<Histogram*> hist_set_;
};


enum COUNTER_STATS {
	FIND_HATBLE_NUM = 0,
	FIND_PLN_HTABLE_NUM,
	FIND_ISN_FROM_CACHE,
	FIND_SEQ_NUM,
	GOT_NOT_FOUND,
	GOT_WRONG_VALUE,
	CFLUSH_NUM,
	CFLUSH_SIZE,
	MFENCE_NUM,
	SCAN_ORDER_ERROR_NUM,
	UPDATE_FAILED,
	GOT_NOT_FOUND_AFTER_UPDATE,
	GOT_WRONG_VALUE_AFTER_UPDATE,
	DELETE_FAILED,
	GOT_FOUND_AFTER_DETELE,
	INSTALL_CMAP_RETRY,
	SEARCH_LIST_2_X1,
	SEARCH_LIST_2_X2,


	MAX_COUNTER_NUM,
};

static std::string COUNTER_STATS_STRING[] = {
	"FIND_HATBLE_NUM",
	"FIND_PLN_HTABLE_NUM",
	"FIND_ISN_FROM_CACHE",
	"FIND_SEQ_NUM",
	"GOT_NOT_FOUND",
	"GOT_WRONG_VALUE",
	"CFLUSH_NUM",
	"CFLUSH_SIZE",
	"MFENCE_NUM",
	"SCAN_ORDER_ERROR_NUM",
	"UPDATE_FAILED",
	"GOT_NOT_FOUND_AFTER_UPDATE",
	"GOT_WRONG_VALUE_AFTER_UPDATE",
	"DELETE_FAILED",
	"GOT_FOUND_AFTER_DETELE",
	"INSTALL_CMAP_RETRY",
	"SEARCH_LIST_2_X1",
	"SEARCH_LIST_2_X2",

	"MAX_COUNTER_NUM"
};

class CounterSet {
  public:
	CounterSet() {
		for (int i = 0; i < COUNTER_STATS::MAX_COUNTER_NUM; ++i) {
			counter_set_.emplace_back(new Counter(COUNTER_STATS_STRING[i]));
		}
	};
	~CounterSet() {
		for (auto x : counter_set_) {
			delete x;
		}
	};

	void Clear() {
		for (auto x : counter_set_) {
			x->Clear();
		}
	}

	void AddNewCounter(std::string name) {
		counter_set_.emplace_back(new Counter(name));
	}

	void Add(size_t pos, uint64_t t = 1) {
		counter_set_[(size_t)pos]->Add(t);
	}

	void PrintResult() {
		for (size_t i = 0; i < counter_set_.size(); ++i) {
			counter_set_[i]->PrintResult();
			fprintf(stderr, "\n");
		}
	}

  private:
	std::vector<Counter*> counter_set_;
};

