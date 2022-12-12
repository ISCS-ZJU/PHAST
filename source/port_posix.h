#pragma once
#include <pthread.h>

class RWMutex {
  public:
	RWMutex() {
		Init();
	}

	// No copying allowed
	RWMutex(const RWMutex&) = delete;
	void operator=(const RWMutex&) = delete;

	~RWMutex() { pthread_rwlock_destroy(&lock_); }

	void Init() {
		pthread_rwlock_init(&lock_, NULL);
#ifndef NDEBUG
		locked_ = false;
#endif
	}

	void ReadLock() {
		pthread_rwlock_rdlock(&lock_);
	}

	void WriteLock() {
		pthread_rwlock_wrlock(&lock_);
#ifndef NDEBUG
		locked_ = true;
#endif
	}

	void ReadUnlock() {
		pthread_rwlock_unlock(&lock_);
	}

	void WriteUnlock() {
		pthread_rwlock_unlock(&lock_);
#ifndef NDEBUG
		locked_ = false;
#endif
	}

	bool AssertReadHeld() {
#ifndef NDEBUG
		return (locked_ == false);
#endif
		return true;
	}

	bool AssertWriteHeld() {
#ifndef NDEBUG
		return (locked_ == true);
#endif
		return true;
	}

  private:
	pthread_rwlock_t lock_;
#ifndef NDEBUG
	bool locked_;
#endif
};

// Exclusive Mutex
class EXMutex {
  public:
	EXMutex() {
		Init();
	}

	// No copying allowed
	EXMutex(const EXMutex&) = delete;
	void operator=(const EXMutex&) = delete;

	~EXMutex() { pthread_mutex_destroy(&lock_); }

	void Init() {
		pthread_mutex_init(&lock_, NULL);
#ifndef NDEBUG
		locked_ = false;
#endif
	}

	void Lock() {
		pthread_mutex_lock(&lock_);
#ifndef NDEBUG
		locked_ = true;
#endif
	}

	void Unlock() {
		pthread_mutex_unlock(&lock_);
#ifndef NDEBUG
		locked_ = false;
#endif
	}

	bool AssertHeld() {
#ifndef NDEBUG
		return (locked_ == true);
#endif
		return true;
	}

  private:
	pthread_mutex_t  lock_;
#ifndef NDEBUG
	bool locked_;
#endif
};