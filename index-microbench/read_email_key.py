#
# read_email_key.py - This file processes the email insert and workload file
#                     and outputs a file to use the index
#
#
# Run this file with the following arguments:
#
# python read_email_key.py [load file] [workload file] [27MB load file] [output file name]
#

import sys
import os

def read_load_file():
  """
  This function reads the load file and returns a dict that maps a string
  to its line number (i.e. index)
  """
  filename = sys.argv[1]
  if os.path.isfile(filename) is False:
    raise TypeError("Illegal load file: %s" % (filename, ))
  
  fp = open(filename, "r")
  line_num = 0
  ret = {}
  for line in fp:
    line = line.strip()
    index = line.find(" ")
    if index == -1 or \
       line[:index] != "INSERT":
      raise ValueError("Illegal line @ %d" % (line_num, ))

    ret[line[index + 1:]] = line_num

    line_num += 1

  fp.close()
  return ret

def read_new_file():
  """
  This function reads the 27MB file into a list and return
  """
  filename = sys.argv[3]
  if os.path.isfile(filename) is False:
    raise TypeError("Illegal 27M file: %s" % (filename, ))
  
  fp = open(filename, "r")
  ret = []
  for line in fp:
    ret.append(line.strip())
  
  fp.close()
  return ret


def read_txn_file(load_dict, new_list):
  """
  This function reads the transaction file, and it also outputs a new file
  regarding
  """
  filename1 = sys.argv[2]
  filename2 = sys.argv[4]
  if os.path.isfile(filename1) is False:
    raise TypeError("Illegal txn file: %s" % (filename1, ))
  elif os.path.isdir(filename2) is True:
    raise TypeError("Illegal output file: %s" % (filename2, ))

  fp1 = open(filename1, "r")
  fp2 = open(filename2, "w")

  line_num = 0
  max_index = len(new_list)
  for line in fp1:
    if line_num != 0:
      fp2.write("\n")

    line = line.strip()
    index = line.find(" ")
    if index == -1:
      raise ValueError("Illegal line @ %d" % (line_num, ))
   
    op = line[:index] 
    out_s = op + " "

    if op == "SCAN":
      index2 = line.find(" ", index + 1)
      if index2 == -1: 
        raise ValueError("Illegal scan line @ %d" % (line_num, ))
      key = line[index + 1:index2]
    else:
      key = line[index + 1:]

    if op != "INSERT":
      key_index = load_dict.get(key, None)
      if key_index is None:
        raise ValueError("Key %s @ %d does not exist" % (key, line_num))
    
      if key_index >= max_index:
        key_index %= max_index
    
      out_s += new_list[key_index]
    else:
      out_s = line

    if op == "SCAN":
      out_s = out_s + line[index2:]

    fp2.write(out_s)
    
    line_num += 1

  fp1.close()
  fp2.close()
  
  return

def convert_new_list(new_list):
  """
  This function converts the new list into an output file which loads
  all strings
  """
  filename = sys.argv[4]
  if os.path.isdir(filename) is True:
    raise TypeError("Illegal output file: %s" % (filename, ))
  
  fp = open(filename, "w")
  line_num = 0
  for line in new_list:
    if line_num != 0:
      fp.write("\n")
    
    s = "INSERT " + line
    fp.write(s)
    line_num += 1
  
  fp.close()
  return


if len(sys.argv) != 5:
  print("This program must take four arguments!")
  print("python read_email_key.py [load file] [workload file] [27MB load file] [output file]")
  print("  If the first two is 'none' then we just convert the 27MB file")
  sys.exit(1)

if sys.argv[2] == "none" and sys.argv[1] == "none":
  print("Converting the load file")
  new_list = read_new_file()
  convert_new_list(new_list)
else:
  load_dict = read_load_file()
  print("Read %d items" % (len(load_dict), ))
  new_list = read_new_file()
  print("Read %d new strings" % (len(new_list), ))
  read_txn_file(load_dict, new_list)
