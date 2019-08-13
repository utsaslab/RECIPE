#include "masstree.h"
#include <string>
#include <random>

using namespace std;

int main(void)
{
    masstree::leafnode *new_root = new masstree::leafnode;
    masstree::masstree tree(new_root);
    char key1[] = "abcdefghijklmnopqrstu1";
    char key2[] = "abcdefghijklmnopqrstu2";

    mt19937 generator{random_device{}()};
    //modify range according to your need "A-Z","a-z" or "0-9" or whatever you need.
    uniform_int_distribution<int> distribution{'a', 'z'};

    auto generate_len = 23; //modify length according to your need
    string rand_str(generate_len, '\0');
    for(auto& dis: rand_str)
        dis = distribution(generator);

    tree.put(key1, (void *)key1);
    tree.put(key2, (void *)key2);

    tree.get(key1);
    tree.get(key2);
    return 0;
}
