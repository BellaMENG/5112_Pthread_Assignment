/*
 * Name: MENG Zihan
 * Student id: 20412027
 * ITSC email: zmengaa@connect.ust.hk
 *
 * Please only change this file and do not change any other files.
 * Feel free to change/add any helper functions.
 *
 * COMPILE: g++ -lstdc++ -std=c++11 -lpthread clustering_pthread_skeleton.cpp -main.cpp -o pthread
 * RUN:     ./pthread <path> <epsilon> <mu> <num_threads>
 */

#include <pthread.h>
#include <boost/pending/detail/disjoint_sets.hpp>
#include "clustering.h"

int global_num_vs, global_num_es, *global_nbr_offs = nullptr, *global_nbrs = nullptr;

float global_epsilon;
int global_mu;
int num_pivots;
bool *pivots = nullptr;
int *num_sim_nbrs = nullptr;
int **sim_nbrs = nullptr;

int *parent = nullptr;
int *index2node = nullptr;
pthread_rwlock_t    rwlock;

struct AllThings{
    int num_threads;
    int my_rank;
    
    AllThings(int inum_threads, int imy_rank){
        num_threads = inum_threads;
        my_rank = imy_rank;
    };
};

void findPivots(int local_num_vs, int start, int end) {
    for (int i = start; i < end; ++i) {
        int *left_start = &global_nbrs[global_nbr_offs[i]];
        int *left_end = &global_nbrs[global_nbr_offs[i + 1]];
        int left_size = left_end - left_start;
        
        sim_nbrs[i] = new int[left_size];
        // loop over all neighbors of i
        for (int *j = left_start; j < left_end; j++) {
            int nbr_id = *j;
            
            int *right_start = &global_nbrs[global_nbr_offs[nbr_id]];
            int *right_end = &global_nbrs[global_nbr_offs[nbr_id + 1]];
            int right_size = right_end - right_start;
            
            // compute the similarity
            int num_com_nbrs = get_num_com_nbrs(left_start, left_end, right_start, right_end);
            
            float sim = (num_com_nbrs + 2) / std::sqrt((left_size + 1.0) * (right_size + 1.0));
            
            if (sim > global_epsilon) {
                sim_nbrs[i][num_sim_nbrs[i]] = nbr_id;
                num_sim_nbrs[i]++;
            }
        }
        if (num_sim_nbrs[i] > global_mu) {
            num_pivots++;
            pivots[i] = true;
        }
    }

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

void print_cluster_result(int* parent, int size) {
    for (int i = 0; i < size; ++i) {
        cout << find_set(parent, i) << " ";
    }
    cout << endl;
}

void *clusterPivots(void* allthings) {
    AllThings *all = (AllThings *) allthings;
    
    return 0;
}


void *parallel(void* allthings){
    AllThings *all = (AllThings *) allthings;
        
    // hints: union-found, set prioriteis, drawer principles
    // two stages:
    // stage 1: find the cores/pivots
    int local_num_vs = global_num_vs/all->num_threads + 1;
    int start = all->my_rank*local_num_vs;
    int end = (all->my_rank + 1)*local_num_vs;
    if (end > global_num_vs)
        end = global_num_vs;
    
    findPivots(local_num_vs, start, end);
    // stage 2: expand the clusters from cores/pivots by DFS or BFS
    // use DFS to traverse all pivots and assign cluster id to them
    // problem is how to perform dfs with multi-thread
    // Boost graph distributed algorithms
    // use an array to record the connectivity
    // agglomerative clustering?
    // greedy agglomerative method introduced by Clauset et al.
    
    // how to remember the linking between different threads?
    return 0;
}

int *scan(float epsilon, int mu, int num_threads, int num_vs, int num_es, int *nbr_offs, int *nbrs){
    // assign these values to the global variables:
    global_num_vs = num_vs;
    global_num_es = num_es;
    global_nbr_offs = nbr_offs;
    global_nbrs = nbrs;
    global_epsilon = epsilon;
    global_mu = mu;
    num_pivots = 0;
    
    pivots = (bool*)malloc(num_vs*sizeof(bool));
    num_sim_nbrs = (int*)malloc(num_vs*sizeof(int));
    sim_nbrs = (int**)malloc(num_vs*sizeof(int));
    
    pthread_rwlock_init(&rwlock, NULL);
    
    long thread;
    pthread_t* thread_handles = (pthread_t*) malloc(num_threads*sizeof(pthread_t));
    int *cluster_result = new int[num_vs];
    
    for (thread = 0; thread < num_threads; thread++) {
        pthread_create(&thread_handles[thread], NULL, parallel, (void *) new AllThings(num_threads, thread));
    }
    
    for (thread = 0; thread < num_threads; thread++) {
        pthread_join(thread_handles[thread], NULL);
    }
    
    index2node = (int*)malloc(num_pivots*sizeof(int));
    int index = 0;
    for (int i = 0; i < num_vs; ++i) {
        if (pivots[i] == true) {
            index2node[index] = i;
            index++;
        }
    }
    parent = (int*)malloc(pivots)
    for (thread = 0; thread < num_threads; thread++) {
        pthread_create(&thread_handles[thread], NULL, clusterPivots, (void *) new AllThings(num_threads, thread));
    }
    
    for (thread = 0; thread < num_threads; thread++) {
        pthread_join(thread_handles[thread], NULL);
    }
    
    
#ifdef DEBUG
    if (global_num_vs <= 50) {
        for (int i = 0; i < global_num_vs; ++i) {
            std::cout << pivots[i] << " ";
        }
        cout << endl;
    }
#endif
    return cluster_result;
}

