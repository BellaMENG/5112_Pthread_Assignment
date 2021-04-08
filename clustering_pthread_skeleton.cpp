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
#include "clustering.h"

int global_num_vs, global_num_es, *global_nbr_offs = nullptr, *global_nbrs = nullptr;

float global_epsilon;
int global_mu;
int num_pivots;
int num_sim_edges;
bool *pivots = nullptr;
int *num_sim_nbrs = nullptr;
int **sim_nbrs = nullptr;

int *parent = nullptr;
pthread_rwlock_t    rwlock;
pthread_barrier_t barrier;

using namespace std;

struct AllThings{
    int num_threads;
    int my_rank;
    int* cluster_result;
    
    AllThings(int inum_threads, int imy_rank, int* icluster_result = nullptr){
        num_threads = inum_threads;
        my_rank = imy_rank;
        cluster_result = icluster_result;
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

void clusteringEdges(int start, int end) {
    for (int i = start; i < end; ++i) {
        if (!pivots[i])
            continue;
        for (int j = 0; j < num_sim_nbrs[i]; ++j) {
            int nbr_id = sim_nbrs[i][j];
            if (nbr_id <= i || !pivots[nbr_id])
                continue;
            pthread_rwlock_wrlock(&rwlock);
            union_sets(parent, i, nbr_id);
            pthread_rwlock_unlock(&rwlock);
        }
    }
}

void clusteringResult(int* cluster_result, int start, int end) {
    for (int i = start; i < end; ++i) {
        if (pivots[i]) {
            cluster_result[i] = find_set(parent, i);
        } else {
            cluster_result[i] = -1;
        }
    }
}

void *parallel(void* allthings){
    AllThings *all = (AllThings *) allthings;
        
    
    int local_num_vs = global_num_vs/all->num_threads;
    int remainder = global_num_vs % all->num_threads;
    int start, end;
    if (all->my_rank < remainder) {
        local_num_vs++;
        start = all->my_rank * local_num_vs;
    } else {
        start = global_num_vs - (all->num_threads - all->my_rank)*local_num_vs;
    }
    
    end = start + local_num_vs;
    
    // stage 1: find the cores/pivots
    findPivots(local_num_vs, start, end);

    pthread_barrier_wait(&barrier);
    for (int i = start; i < end; ++i) {
        if (pivots[i]) {
            parent[i] = i;
        } else {
            parent[i] = -1;
        }
    }
    
    pthread_barrier_wait(&barrier);
    // stage 2: cluster pivots
    clusteringEdges(start, end);
    pthread_barrier_wait(&barrier);
    // stage 3: get results
    clusteringResult(all->cluster_result, start, end);

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
    sim_nbrs = (int**)malloc(num_vs*sizeof(int*));
    
    parent = (int*)calloc(num_vs, sizeof(int));

    pthread_rwlock_init(&rwlock, NULL);
    pthread_barrier_init(&barrier, NULL, num_threads);
    
    long thread;
    pthread_t* thread_handles = (pthread_t*) malloc(num_threads*sizeof(pthread_t));
    int *cluster_result = new int[num_vs];
    for (thread = 0; thread < num_threads; thread++) {
        pthread_create(&thread_handles[thread], NULL, parallel, (void *) new AllThings(num_threads, thread, cluster_result));
    }
    
    for (thread = 0; thread < num_threads; thread++) {
        pthread_join(thread_handles[thread], NULL);
    }
            
    pthread_rwlock_destroy(&rwlock);
    pthread_barrier_destroy(&barrier);
    free(thread_handles);
    for (auto i = 0; i < num_vs; i++) {
        free(sim_nbrs[i]);
    }
    free(sim_nbrs);
    free(pivots);
    free(num_sim_nbrs);
    free(parent);
    return cluster_result;
}

