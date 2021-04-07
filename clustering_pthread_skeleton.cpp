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
bool *visited = nullptr;

int *parent = nullptr;
//int *index2node = nullptr;
//int *node2index = nullptr;
pthread_rwlock_t    rwlock;
pthread_mutex_t union_mutex;
pthread_barrier_t barrier;

using namespace std;

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
        
        pthread_mutex_lock(&union_mutex);
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
        pthread_mutex_unlock(&union_mutex);
        if (num_sim_nbrs[i] > global_mu) {
            num_pivots++;
            pivots[i] = true;
        }
    }

}


void make_set(int* parent, int v) {
//    pthread_rwlock_wrlock(&rwlock);
    parent[v] = v;
//    pthread_rwlock_unlock(&rwlock);
}

int find_set(int* parent, int v) {
    if (v == parent[v])
        return v;
    return parent[v] = find_set(parent, parent[v]);
}

void union_sets(int* parent, int v, int w) {
//    pthread_rwlock_wrlock(&rwlock);
    v = find_set(parent, v);
    w = find_set(parent, w);
//    cout << "v: " << v << " w: " << w << endl;
//    pthread_rwlock_unlock(&rwlock);
    if (v != w) {
//        pthread_rwlock_wrlock(&rwlock);
        if (v > w)
            parent[v] = w;
        else
            parent[w] = v;
//        pthread_rwlock_unlock(&rwlock);
    }
}

void print_parent() {
    for (int i = 0; i < global_num_vs; ++i) {
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

void dfs(int curr_id, int cluster_id, int *num_sim_nbrs, int **sim_nbrs, bool *visited, bool* pivots, int *parent) {
    for (int i = 0; i < num_sim_nbrs[curr_id]; ++i) {
        int nbr_id = sim_nbrs[curr_id][i];
        
        if (pivots[nbr_id] && !visited[nbr_id]) {
            cout << "curr_id: " << curr_id << " nbr id: " << nbr_id << endl;
            pthread_rwlock_wrlock(&rwlock);
            visited[nbr_id] = true;
//            cout << "visited " << nbr_id << endl;
            union_sets(parent, curr_id, nbr_id);
//            cout << "unioned: " << curr_id << " and " << nbr_id << endl;
            pthread_rwlock_unlock(&rwlock);
            dfs(nbr_id, cluster_id, num_sim_nbrs, sim_nbrs, visited, pivots, parent);
        }
    }
    cout << endl;
}

void clusterPivots(int start, int end) {
//    AllThings *all = (AllThings *) allthings;
//    pthread_rwlock_rdlock(&rwlock);
//    int local_n = global_num_vs/all->num_threads + 1;
//    int start = all->my_rank*local_n;
//    int end = (all->my_rank + 1)*local_n;
//    if (end > global_num_vs)
//        end = global_num_vs;

    for (int i = start; i < end; ++i) {
        std::cout << "node " << i << ": " << pivots[i] << "   ";
        for (int j = 0; j < num_sim_nbrs[i]; ++j) {
            std::cout << sim_nbrs[i][j] << " ";
        }
        cout << endl;
    }
    
//    pthread_rwlock_unlock(&rwlock);
    for (int i = start; i < end; ++i) {
//        cout << "i: " << i << endl;
        if (visited[i] || !pivots[i])
            continue;
        pthread_rwlock_wrlock(&rwlock);
        visited[i] = true;
        pthread_rwlock_unlock(&rwlock);
        dfs(i, i, num_sim_nbrs, sim_nbrs, visited, pivots, parent);
    }
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
    pthread_barrier_wait(&barrier);
    
    for (int i = start; i < end; ++i) {
        std::cout << "node " << i << ": " << pivots[i] << "   ";
        for (int j = 0; j < num_sim_nbrs[i]; ++j) {
            std::cout << sim_nbrs[i][j] << " ";
        }
        cout << endl;
    }
    pthread_barrier_wait(&barrier);
//    cout << sim_nbrs[14][0] << endl;
    
//    for (int i = start; i < end; ++i) {
//        if (pivots[i])
//            make_set(parent, i);
//        else
//            parent[i] = -1;
//    }
    pthread_mutex_lock(&union_mutex);
    for (int i = start; i < end; ++i) {
        if (pivots[i]) {
            parent[i] = i;
        } else {
            parent[i] = -1;
        }
    }
    
    pthread_mutex_unlock(&union_mutex);
    pthread_barrier_wait(&barrier);
    cout << "here" << endl;
    cout << sim_nbrs[13][0] << endl;
    cout << sim_nbrs[14][0] << endl;
    clusterPivots(start, end);
    pthread_barrier_wait(&barrier);
    // stage 2: cluster pivots
    
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
    
    pivots = (bool*)calloc(num_vs, sizeof(bool));
    num_sim_nbrs = (int*)calloc(num_vs, sizeof(int));
    sim_nbrs = (int**)malloc(num_vs*sizeof(int));
    
    parent = (int*)calloc(num_vs, sizeof(int));
    visited = (bool*)calloc(num_vs, sizeof(bool));

    pthread_rwlock_init(&rwlock, NULL);
    pthread_mutex_init(&union_mutex, NULL);
    pthread_barrier_init(&barrier, NULL, num_threads);
    
    long thread;
    pthread_t* thread_handles = (pthread_t*) malloc(num_threads*sizeof(pthread_t));
    int *cluster_result = new int[num_vs];
    for (thread = 0; thread < num_threads; thread++) {
        pthread_create(&thread_handles[thread], NULL, parallel, (void *) new AllThings(num_threads, thread));
    }
    
    for (thread = 0; thread < num_threads; thread++) {
        pthread_join(thread_handles[thread], NULL);
    }
    

#ifdef DEBUG
    cout << "global_num_vs: " << global_num_vs << endl;
    if (global_num_vs <= 50) {
        for (int i = 0; i < global_num_vs; ++i) {
            std::cout << pivots[i] << " ";
        }
        cout << endl;
    }
    
#endif
    
//    cout << sim_nbrs[14][0] << endl;
//    for (thread = 0; thread < num_threads; thread++) {
//        pthread_create(&thread_handles[thread], NULL, clusterPivots, (void *) new AllThings(num_threads, thread));
//    }
//
//    for (thread = 0; thread < num_threads; thread++) {
//        pthread_join(thread_handles[thread], NULL);
//    }
#ifdef DEBUG
//    print_cluster_result(parent, num_vs);
//    print_cluster_result(parent, num_vs);
#endif
    
    pthread_rwlock_destroy(&rwlock);
    pthread_mutex_destroy(&union_mutex);
    pthread_barrier_destroy(&barrier);
    free(thread_handles);
    return cluster_result;
}

