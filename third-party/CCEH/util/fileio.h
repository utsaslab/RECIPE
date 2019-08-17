#ifndef UTIL_FILE_IO_H_
#define UTIL_FILE_IO_H_

#include <fstream>
#include <cassert>

// @brief: reads a file from path to a memory and returns the pointer
// @param path: the path of a file
// @param len: the length of a file
char* File2Arr(const char* path, size_t& len) {
  std::ifstream ifs(path);
  assert(ifs.good());

  ifs.seekg(0, std::ios_base::end);
  len = ifs.tellg();
  ifs.seekg(0, std::ios_base::beg);  // rewind

  char* ret = new char[len];
  ifs.read(ret, len);
  ifs.close();

  return ret;
}

#endif  // UTIL_FILE_IO_H_
