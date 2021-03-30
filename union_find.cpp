//
//  union_find.cpp
//  
//
//  Created by MENG Zihan on 3/29/21.
//

#include "union_find.hpp"
#include <iostream>

using namespace std;

int* parent = nullptr;
//int* rank = nullptr;

void make_set(int*, int);
int find_set(int*, int);
void union_sets(int*, int, int);
void print_parent(int*, int);
void test_ds();

int main(int argc, char** argv) {
    test_ds();
    return 0;
}

void make_set(int* parent, int v) {
    parent[v] = v;
}

int find_set(int* parent, int v) {
    if (v == parent[v])
        return v;
    return parent[v] = find_set(parent, parent[v]);
}

void union_sets(int* parent, int v, int w) {
    v = find_set(parent, v);
    w = find_set(parent, w);
    if (v != w) {
        if (v > w)
            parent[v] = w;
        else
            parent[w] = v;
    }
}

void print_parent(int* parent, int size) {
    for (int i = 0; i < size; ++i) {
        cout << parent[i] << " ";
    }
    cout << endl;
}

void print_cluster_result(int* parent, int size) {
    for (int i = 0; i < size; ++i) {
        cout << find_set(parent, i) << " ";
    }
    cout << endl;
}

void test_ds() {
    
    parent = (int*)malloc(5*sizeof(int));
    int size = 5;
    for (int i = 0; i < size; ++i) {
        make_set(parent, i);
    }
    print_cluster_result(parent, size);

    union_sets(parent, 0, 1);
    print_cluster_result(parent, size);
    union_sets(parent, 2, 3);
    print_cluster_result(parent, size);
    union_sets(parent, 0, 3);
    print_cluster_result(parent, size);
    union_sets(parent, 0, 4);
    print_cluster_result(parent, size);
}

