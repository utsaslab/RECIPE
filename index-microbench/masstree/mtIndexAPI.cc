#include <iostream>
#include "mtIndexAPI.hh"

volatile uint64_t globalepoch = 1;
volatile bool recovering = false;
kvtimestamp_t initial_timestamp;
kvepoch_t global_log_epoch = 0;

/*
template <typename T>
void mt_index<T>::setup() {
  ti_ = new threadinfo();
  ti_->rcu_start();
  table_ = new T;
  table_->initialize(*ti_);
}

template <typename T>
void mt_index<T>::put(const Str &key, const Str &value) {
  q_[0].run_replace(table_->table(), key, value, *ti_);
}

template <typename T>
void mt_index<T>::put(const char *key, int keylen, const char *value, int valuelen) {
  put(Str(key, keylen), Str(value, valuelen));
}

template <typename T>
bool mt_index<T>::get(const char *key, int keylen, Str &value) {
  return q_[0].run_get1(table_->table(), Str(key, keylen), 0, value, *ti_);
}
*/
/*
template <typename T>
bool mt_index<T>::remove(const Str &key) {
  return q_[0].run_remove(table_->table(), key, *ti_);
}

template <typename T>
bool mt_index<T>::remove(const char *key, int keylen) {
  return remove(Str(key, keylen));
}
*/
/*
int main () {
  std::cout << "test\n";
  mt_index<Masstree::default_table> mti;
 
  mti.setup();

  mti.put("huanchen", 8, "yingjie", 7);
  mti.put("zhuo", 4, "yangzi", 6);
  mti.put("julian", 6, "wenlu", 5);
  Str value;
  bool get_success;
  get_success = mti.get("huanchen", 8, value);
  std::cout << get_success << "\t" << value.s << "\n";
  get_success = mti.get("zhuo", 4, value);
  std::cout << get_success << "\t" << value.s << "\n";
  get_success = mti.get("julian", 6, value);
  std::cout << get_success << "\t" << value.s << "\n";
  get_success = mti.get("dave", 4, value);
  std::cout << get_success << "\t" << value.s << "\n";
 
  return 0;
}
*/
