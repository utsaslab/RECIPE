
#include "rotate-skiplist.h"

using namespace rotate_skiplist;

/////////////////////////////////////////////////////////////////////
// Global varialbes: GCGlobalState
/////////////////////////////////////////////////////////////////////

// Initialized once on Init() call of class GCGlobalState
GCGlobalState *GCGlobalState::global_state_p = nullptr;

/////////////////////////////////////////////////////////////////////
// Global varialbes: ThreadState
/////////////////////////////////////////////////////////////////////

// Three global variables defined within thread state object
// which mains the global linked list of thread objects
pthread_key_t ThreadState::thread_state_key{};

// Will initialize these two in the global initializer
std::atomic<unsigned int> ThreadState::next_id{0U};
std::atomic<ThreadState *> ThreadState::thread_state_head_p{nullptr};

// Has not been initialized yet
bool ThreadState::inited = false;