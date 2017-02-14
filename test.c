#include <zookeeper/zookeeper.h>
#include <zookeeper/proto.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>

void my_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    printf("watcher: %d %s\n", type, path);
}

int my_node_name_cmp(const void *pname1, const void *pname2) {
    return strcmp(*(const char **)pname1, *(const char **)pname2);
}

void my_lock_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    pthread_mutex_t *mutex;

    mutex = (pthread_mutex_t *)watcherCtx;

    printf("\nnode changed!\n");

    pthread_mutex_unlock(mutex);
}

int my_get_lock(zhandle_t *zh, char *my_node_name) {
    int i = 0;
    int stat = 0;
    char path[255];
    struct String_vector nodes = {0, NULL};
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

    stat = zoo_get_children(zh, "/locks/jobmaker", 0, &nodes);

    if (ZOK != stat) {
        printf("Zookeeper: get children for /locks/jobmaker failed!");
        exit(1);
    }

    // it should not be 0 because of a new node added for the process.
    assert(nodes.count > 0);

    for (i=0; i<nodes.count; i++) {
        printf("nodes %d: %s\n", i, nodes.data[i]);
    }

    qsort(nodes.data, nodes.count, sizeof(nodes.data), my_node_name_cmp);

    for (i=0; i<nodes.count; i++) {
        printf("sorted nodes %d: %s\n", i, nodes.data[i]);
    }
    
    printf("cmp: %s - %s\n", my_node_name, nodes.data[0]);

    if (0 == strcmp(my_node_name, nodes.data[0])) {
        printf("get the lock\n");

        return 1;
    }
    else {
        printf("wait for lock\n");

        // find the process itself
        for (i=0; strcmp(my_node_name, nodes.data[i]); i++);

        // it should not be the first node
        assert(i > 0);

        // get the before node path
        strcpy(path, "/locks/jobmaker/");
        strcat(path, nodes.data[i - 1]);

        // watch the before node
        stat = zoo_wexists(zh, path, my_lock_watcher, &mutex, NULL);

        printf("watch node %s -> %s\n", path, zerror(stat));

        // block the thread until get the lock
        // lock twice so the thread blocked
        pthread_mutex_lock(&mutex);
        pthread_mutex_lock(&mutex);
        
        printf("\nI'm wake!!!\n");

        return 0;
    }

}

int main() {
    zhandle_t *zh = NULL;
    int stat = 0;
    char path[255] = {0};
    int i = 0;

    zh = zookeeper_init("127.0.0.1:2181", my_watcher, 10000, NULL, NULL, 0);

    printf("----------------- Create Parent ----------------\n");
    zoo_create(zh, "/locks", NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    zoo_create(zh, "/locks/jobmaker", NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);

    stat = zoo_exists(zh, "/locks/jobmaker", 0, NULL);
    
    if (ZOK != stat) {
        printf("Zookeeper: create /locks/jobmaker failed!");
        exit(1);
    }

    printf("----------------- Create ----------------\n");
    stat = zoo_create(zh, "/locks/jobmaker/node", NULL, -1, &ZOO_READ_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE, path, 255);
    printf("stat: %s -> %s\n", zerror(stat), path);

    stat = zoo_exists(zh, path, 0, NULL);

    if (ZOK != stat) {
        printf("Zookeeper: create /locks/jobmaker/node failed!");
        exit(1);
    }

    // wait the lock
    // it isn't a busy waiting because my_get_lock() will
    // block itself with pthread_mutex_lock().
    while(!my_get_lock(zh, path + 16));

    printf("------ Press Enter and Exit");
    getchar();

    // remove the lock
    zoo_delete(zh, path, -1);

    zookeeper_close(zh);

    return 0;
}
