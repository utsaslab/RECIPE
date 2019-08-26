#include <cstdio>
#include <iostream>

void func(int* &value) {
    int a = 3;
    value = &a;
}

int main(void)
{
    int a = 5;
    int *p = &a;

    func(p);
    printf("%d\n", *p);

    return 0;
}
